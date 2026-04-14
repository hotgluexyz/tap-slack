import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import pendulum
import pytz

from hotglue_singer_sdk import Stream
from hotglue_singer_sdk.authenticators import BearerTokenAuthenticator
from hotglue_singer_sdk.exceptions import FatalAPIError
from hotglue_singer_sdk.helpers._util import utc_now

from tap_slack.auth import SlackOAuthAuthenticator
from tap_slack.transform import transform_json

LOGGER = logging.getLogger(__name__)
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
utc = pytz.UTC


class SlackStream(Stream):

    @property
    def schema_filepath(self) -> Path:
        return Path(__file__).parent / "schemas" / f"{self.name}.json"

    @property
    def authenticator(self):
        if self.config.get("refresh_token"):
            # Token rotation enabled — cache the authenticator on the tap so a
            # single refresh covers all streams in the sync run.
            if not hasattr(self._tap, "_oauth_authenticator"):
                self._tap._oauth_authenticator = SlackOAuthAuthenticator(
                    stream=self,
                    auth_endpoint="https://slack.com/api/oauth.v2.access",
                )
            return self._tap._oauth_authenticator
        token = self.config.get("access_token")
        if not token:
            raise FatalAPIError("access_token is required")
        return BearerTokenAuthenticator.create_for_stream(self, token=token)

    @property
    def client(self):
        return self._tap.slack_client

    def _get_date_window_size(self) -> int:
        return int(self.config.get("date_window_size", "7"))

    def _get_absolute_date_range(self, start_date: str):
        lookback_window = int(self.config.get("lookback_window", "14"))
        start_dttm = pendulum.parse(start_date)
        now_dttm = utc_now()
        delta_days = (now_dttm - start_dttm).days
        if delta_days < lookback_window:
            start_ddtm = now_dttm - timedelta(days=lookback_window)
        else:
            start_ddtm = start_dttm
        return start_ddtm, now_dttm

    def _get_channels(self):
        if "channels" in self.config:
            for channel_id in self.config.get("channels"):
                yield from self.client.get_channel(include_num_members=0, channel=channel_id)
        else:
            types = "public_channel"
            if self.config.get("private_channels") == "true":
                types = "public_channel,private_channel"
            exclude_archived = self.config.get("exclude_archived", "false")
            for page in self.client.get_all_channels(types=types, exclude_archived=exclude_archived):
                for channel in page.get("channels", []):
                    yield channel

    @staticmethod
    def _unix_str_to_iso(ts_str) -> Optional[str]:
        """Convert a unix-seconds value (int or decimal string) to ISO 8601."""
        if ts_str is None:
            return ts_str
        try:
            if isinstance(ts_str, int):
                return datetime.utcfromtimestamp(ts_str).strftime("%Y-%m-%dT%H:%M:%SZ")
            if isinstance(ts_str, str):
                return datetime.utcfromtimestamp(int(ts_str.partition(".")[0])).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
        except (ValueError, TypeError):
            pass
        return ts_str

    def _convert_date_fields(self, record: dict, date_fields: list) -> dict:
        for field in date_fields:
            if record.get(field) is not None:
                record[field] = self._unix_str_to_iso(record[field])
        return record


class ConversationsStream(SlackStream):
    name = "channels"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    date_fields = ["created"]

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        for channel in self._get_channels():
            records = transform_json(stream=self.name, data=[channel], date_fields=self.date_fields)
            for record in records:
                yield self._convert_date_fields(record, self.date_fields)


class ConversationMembersStream(SlackStream):
    name = "channel_members"
    primary_keys = ["channel_id", "user_id"]
    replication_method = "FULL_TABLE"

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        for channel in self._get_channels():
            channel_id = channel.get("id")
            members_cursor = self.client.get_channel_members(channel_id)
            for page in members_cursor:
                for member in page.get("members", []):
                    yield {"channel_id": channel_id, "user_id": member}


class ConversationHistoryStream(SlackStream):
    name = "messages"
    primary_keys = ["channel_id", "ts"]
    replication_key = "ts"
    date_fields = ["ts"]

    def _get_channel_bookmark(self, channel_id: str) -> str:
        return (
            self._tap.state
            .get("bookmarks", {})
            .get(self.name, {})
            .get(channel_id)
            or self.config.get("start_date")
        )

    def _set_channel_bookmark(self, channel_id: str, value: str) -> None:
        state = self._tap.state
        state.setdefault("bookmarks", {}).setdefault(self.name, {})[channel_id] = value
        self._write_state_message()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        date_window_size = self._get_date_window_size()

        for channel in self._get_channels():
            channel_id = channel.get("id")
            bookmark = self._get_channel_bookmark(channel_id)
            start, end = self._get_absolute_date_range(bookmark)

            date_window_start = start
            date_window_end = start + timedelta(days=date_window_size)
            min_bookmark = start
            max_bookmark = start

            while date_window_start < date_window_end:
                messages = self.client.get_messages(
                    channel=channel_id,
                    oldest=int(date_window_start.timestamp()),
                    latest=int(date_window_end.timestamp()),
                )

                if messages:
                    for page in messages:
                        raw_messages = transform_json(
                            stream=self.name,
                            data=page.get("messages"),
                            date_fields=self.date_fields,
                            channel_id=channel_id,
                        )
                        for message in raw_messages:
                            # thread_ts is stored as the raw decimal string by
                            # transform_json; use it for API calls and bookmark maths.
                            raw_thread_ts = message.get("thread_ts", "")
                            ts_int = int(raw_thread_ts.partition(".")[0]) if raw_thread_ts else 0

                            if ts_int >= start.timestamp():
                                record_dt = datetime.utcfromtimestamp(ts_int).replace(tzinfo=utc)
                                if record_dt > max_bookmark.replace(tzinfo=utc):
                                    max_bookmark = datetime.fromtimestamp(ts_int)
                                elif record_dt < min_bookmark.replace(tzinfo=utc):
                                    min_bookmark = datetime.fromtimestamp(ts_int)

                                yield self._convert_date_fields(
                                    {"channel_id": channel_id, **message}, self.date_fields
                                )

                    self._set_channel_bookmark(channel_id, min_bookmark.strftime(DATETIME_FORMAT))
                    date_window_start = date_window_end
                    date_window_end = date_window_start + timedelta(days=date_window_size)
                    if date_window_end > end:
                        date_window_end = end
                else:
                    date_window_start = date_window_end

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {
            "channel_id": record["channel_id"],
            # thread_ts is the raw decimal string — required by conversations.replies
            "thread_ts": record.get("thread_ts"),
        }


class ThreadsStream(SlackStream):
    name = "threads"
    primary_keys = ["channel_id", "ts", "thread_ts"]
    replication_method = "FULL_TABLE"
    parent_stream_type = ConversationHistoryStream
    date_fields = ["ts", "last_read"]

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        channel_id = context["channel_id"]
        ts = context["thread_ts"]
        start, end = self._get_absolute_date_range(self.config.get("start_date"))

        try:
            replies = self.client.get_thread(
                channel=channel_id,
                ts=ts,
                inclusive="true",
                oldest=int(start.timestamp()),
                latest=int(end.timestamp()),
            )
            for page in replies:
                raw_threads = transform_json(
                    stream=self.name,
                    data=page.get("messages", []),
                    date_fields=self.date_fields,
                    channel_id=channel_id,
                )
                for message in raw_threads:
                    yield self._convert_date_fields(message, self.date_fields)
        except Exception:
            LOGGER.exception("Failed to sync thread %s in channel %s", ts, channel_id)


class UsersStream(SlackStream):
    name = "users"
    primary_keys = ["id"]
    replication_key = "updated"
    date_fields = ["updated"]

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        bookmark = self.stream_state.get("replication_key_value") or self.config.get("start_date")

        for page in self.client.get_users(limit=100):
            users = transform_json(stream=self.name, data=page.get("members"), date_fields=self.date_fields)
            for user in users:
                self._convert_date_fields(user, self.date_fields)
                if user.get("updated", "") > bookmark:
                    yield user


class UserGroupsStream(SlackStream):
    name = "user_groups"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        for page in self.client.get_user_groups(
            include_count="true", include_disabled="true", include_user="true"
        ):
            for usergroup in page.get("usergroups", []):
                yield usergroup


class TeamsStream(SlackStream):
    name = "teams"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        for page in self.client.get_teams():
            team = page.get("team")
            if team:
                yield team


class FilesStream(SlackStream):
    name = "files"
    primary_keys = ["id"]
    replication_key = "timestamp"
    date_fields = ["timestamp", "created", "updated", "date_delete"]

    def _get_file_bookmark(self) -> str:
        return self.stream_state.get("window_bookmark") or self.config.get("start_date")

    def _set_file_bookmark(self, value: str) -> None:
        self.stream_state["window_bookmark"] = value
        self._write_state_message()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        date_window_size = self._get_date_window_size()
        bookmark = self._get_file_bookmark()
        start, end = self._get_absolute_date_range(bookmark)

        date_window_start = start
        date_window_end = start + timedelta(days=date_window_size)
        min_bookmark = start
        max_bookmark = start

        while date_window_start < date_window_end:
            files_list = self.client.get_files(
                from_ts=int(date_window_start.timestamp()),
                to_ts=int(date_window_end.timestamp()),
            )

            for page in files_list:
                for file in page.get("files", []):
                    ts_int = int(file.get("timestamp", 0))
                    if ts_int >= start.timestamp():
                        record_dt = datetime.utcfromtimestamp(ts_int).replace(tzinfo=utc)
                        if record_dt > max_bookmark.replace(tzinfo=utc):
                            max_bookmark = datetime.fromtimestamp(ts_int)
                        elif record_dt < min_bookmark.replace(tzinfo=utc):
                            min_bookmark = datetime.fromtimestamp(ts_int)
                        yield self._convert_date_fields(file, self.date_fields)

            self._set_file_bookmark(min_bookmark.strftime(DATETIME_FORMAT))
            date_window_start = date_window_end
            date_window_end = date_window_start + timedelta(days=date_window_size)
            if date_window_end > end:
                date_window_end = end


class RemoteFilesStream(SlackStream):
    name = "remote_files"
    primary_keys = ["id"]
    replication_key = "updated"
    date_fields = ["updated", "timestamp"]

    def _get_remote_file_bookmark(self) -> str:
        return self.stream_state.get("window_bookmark") or self.config.get("start_date")

    def _set_remote_file_bookmark(self, value: str) -> None:
        self.stream_state["window_bookmark"] = value
        self._write_state_message()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        date_window_size = self._get_date_window_size()
        bookmark = self._get_remote_file_bookmark()
        start, end = self._get_absolute_date_range(bookmark)

        date_window_start = start
        date_window_end = start + timedelta(days=date_window_size)
        min_bookmark = start
        max_bookmark = start

        while date_window_start < date_window_end:
            remote_files_list = self.client.get_remote_files(
                from_ts=int(date_window_start.timestamp()),
                to_ts=int(date_window_end.timestamp()),
            )

            for page in remote_files_list:
                remote_files = transform_json(
                    stream=self.name,
                    data=page.get("files"),
                    date_fields=self.date_fields,
                )
                for file in remote_files:
                    ts_int = int(file.get("timestamp", "0").partition(".")[0] or 0)
                    if ts_int >= start.timestamp():
                        record_dt = datetime.utcfromtimestamp(ts_int).replace(tzinfo=utc)
                        if record_dt > max_bookmark.replace(tzinfo=utc):
                            max_bookmark = datetime.fromtimestamp(ts_int)
                        elif record_dt < min_bookmark.replace(tzinfo=utc):
                            min_bookmark = datetime.fromtimestamp(ts_int)
                        yield self._convert_date_fields(file, self.date_fields)

            self._set_remote_file_bookmark(min_bookmark.strftime(DATETIME_FORMAT))
            date_window_start = date_window_end
            date_window_end = date_window_start + timedelta(days=date_window_size)
            if date_window_end > end:
                date_window_end = end


STREAM_TYPES = [
    ConversationsStream,
    ConversationMembersStream,
    ConversationHistoryStream,
    ThreadsStream,
    UsersStream,
    UserGroupsStream,
    TeamsStream,
    FilesStream,
    RemoteFilesStream,
]
