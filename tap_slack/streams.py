import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import pendulum

from hotglue_singer_sdk.authenticators import BearerTokenAuthenticator
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from hotglue_singer_sdk.helpers._util import utc_now
from hotglue_singer_sdk.streams import RESTStream

from tap_slack.auth import SlackOAuthAuthenticator
from tap_slack.transform import transform_json

LOGGER = logging.getLogger(__name__)
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
SLACK_AUTH_ENDPOINT = "https://slack.com/api/oauth.v2.access"


class SlackRESTStream(RESTStream):
    """Shared Slack REST behavior for all streams."""

    url_base = "https://slack.com/api/"
    path = ""
    next_page_token_jsonpath = "$.response_metadata.next_cursor"

    @property
    def schema_filepath(self) -> Path:
        return Path(__file__).parent / "schemas" / f"{self.name}.json"

    @property
    def authenticator(self):
        refresh_token = self._tap.config.get("refresh_token")
        refresh_token_value = getattr(refresh_token, "contents", refresh_token)

        if refresh_token_value:
            if not hasattr(self._tap, "_oauth_authenticator"):
                self._tap._oauth_authenticator = SlackOAuthAuthenticator(
                    stream=self,
                    auth_endpoint=SLACK_AUTH_ENDPOINT,
                )
            return self._tap._oauth_authenticator

        token = self.config.get("access_token")
        if not token:
            raise FatalAPIError("access_token is required")
        return BearerTokenAuthenticator.create_for_stream(self, token=token)

    def validate_response(self, response):
        super().validate_response(response)
        payload = response.json()
        if payload.get("ok", True):
            return

        error = payload.get("error", "unknown_error")
        if error == "ratelimited":
            raise RetriableAPIError(f"Slack API rate limit reached for {self.path}", response)
        if error in {"invalid_auth", "token_revoked", "not_authed", "account_inactive"}:
            raise FatalAPIError(f"Invalid Credentials: {error}")
        raise FatalAPIError(f"Slack API error for {self.path}: {error}")

    def _request_json(self, path: str, params: Optional[dict] = None, context: Optional[dict] = None):
        self.path = path
        decorated_request = self.request_decorator(self._request)
        request = self.build_prepared_request(
            method="GET",
            url=f"{self.url_base}{path}",
            params=params or {},
            headers=self.http_headers,
        )
        response = decorated_request(request, context)
        self.update_sync_costs(request, response, context)
        return response.json()

    def _request_paginated_json(
        self, path: str, params: Optional[dict] = None, context: Optional[dict] = None
    ):
        self.path = path
        next_cursor = None
        decorated_request = self.request_decorator(self._request)
        while True:
            request_params = dict(params or {})
            if next_cursor:
                request_params["cursor"] = next_cursor
            request = self.build_prepared_request(
                method="GET",
                url=f"{self.url_base}{path}",
                params=request_params,
                headers=self.http_headers,
            )
            response = decorated_request(request, context)
            self.update_sync_costs(request, response, context)
            payload = response.json()
            yield payload
            next_cursor = payload.get("response_metadata", {}).get("next_cursor") or None
            if not next_cursor:
                break

    def _get_date_window_size(self) -> int:
        return int(self.config.get("date_window_size", "7"))

    def _get_absolute_date_range(self, start_date: str):
        lookback_window = int(self.config.get("lookback_window", "14"))
        start_dttm = pendulum.parse(start_date)
        now_dttm = utc_now()
        delta_days = (now_dttm - start_dttm).days
        if delta_days < lookback_window:
            start_dttm = now_dttm - timedelta(days=lookback_window)
        return start_dttm, now_dttm

    def _get_channel(self, channel_id: str) -> dict:
        payload = self._request_json(
            "conversations.info",
            params={"channel": channel_id, "include_num_members": 0},
            context={"channel_id": channel_id},
        )
        return payload.get("channel", {})

    def iter_channels(self):
        configured_channels = self.config.get("channels") or []
        if configured_channels:
            for channel_id in configured_channels:
                channel = self._get_channel(channel_id)
                if channel:
                    yield channel
            return

        types = "public_channel,private_channel" if self.config.get("private_channels") == "true" else "public_channel"
        exclude_archived = self.config.get("exclude_archived", "false")
        for page in self._request_paginated_json(
            "conversations.list",
            params={
                "types": types,
                "exclude_archived": exclude_archived,
                "limit": 200,
            },
        ):
            for channel in page.get("channels", []):
                yield channel

    def join_channel(self, channel_id: str) -> dict:
        return self._request_json("conversations.join", params={"channel": channel_id})

    @staticmethod
    def _unix_str_to_iso(timestamp_value):
        """Convert a unix-seconds value (int or decimal string) to ISO 8601."""
        if timestamp_value is None:
            return timestamp_value
        try:
            if isinstance(timestamp_value, int):
                return datetime.utcfromtimestamp(timestamp_value).strftime("%Y-%m-%dT%H:%M:%SZ")
            if isinstance(timestamp_value, str):
                return datetime.utcfromtimestamp(int(timestamp_value.partition(".")[0])).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
        except (TypeError, ValueError):
            return timestamp_value
        return timestamp_value

    def _convert_date_fields(self, record: dict, date_fields: list) -> dict:
        for field in date_fields:
            if record.get(field) is not None:
                record[field] = self._unix_str_to_iso(record[field])
        return record


class ConversationsStream(SlackRESTStream):
    name = "channels"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    date_fields = ["created"]

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        for channel in self.iter_channels():
            records = transform_json(stream=self.name, data=[channel], date_fields=self.date_fields)
            for record in records:
                yield self._convert_date_fields(record, self.date_fields)


class ConversationMembersStream(SlackRESTStream):
    name = "channel_members"
    primary_keys = ["channel_id", "user_id"]
    replication_method = "FULL_TABLE"

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        for channel in self.iter_channels():
            channel_id = channel.get("id")
            if not channel_id:
                continue
            try:
                pages = self._request_paginated_json(
                    "conversations.members",
                    params={"channel": channel_id, "limit": 200},
                    context={"channel_id": channel_id},
                )
                for page in pages:
                    for member in page.get("members", []):
                        yield {"channel_id": channel_id, "user_id": member}
            except FatalAPIError as ex:
                if "fetch_members_failed" in str(ex):
                    LOGGER.warning("Failed to fetch members for channel: %s", channel_id)
                    continue
                raise


class ConversationHistoryStream(SlackRESTStream):
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
        for channel in self.iter_channels():
            channel_id = channel.get("id")
            if not channel_id:
                continue

            bookmark = self._get_channel_bookmark(channel_id)
            start, end = self._get_absolute_date_range(bookmark)
            date_window_start = start
            date_window_end = min(start + timedelta(days=date_window_size), end)
            min_bookmark = start
            max_bookmark = start

            while date_window_start < end:
                try:
                    pages = self._request_paginated_json(
                        "conversations.history",
                        params={
                            "channel": channel_id,
                            "oldest": int(date_window_start.timestamp()),
                            "latest": int(date_window_end.timestamp()),
                            "limit": 200,
                        },
                        context={"channel_id": channel_id},
                    )
                    for page in pages:
                        raw_messages = transform_json(
                            stream=self.name,
                            data=page.get("messages", []),
                            date_fields=self.date_fields,
                            channel_id=channel_id,
                        )
                        for message in raw_messages:
                            raw_thread_ts = message.get("thread_ts", "")
                            ts_int = int(raw_thread_ts.partition(".")[0]) if raw_thread_ts else 0
                            if ts_int >= start.timestamp():
                                record_dt = datetime.fromtimestamp(ts_int, tz=timezone.utc)
                                if record_dt > max_bookmark.replace(tzinfo=timezone.utc):
                                    max_bookmark = datetime.fromtimestamp(ts_int)
                                elif record_dt < min_bookmark.replace(tzinfo=timezone.utc):
                                    min_bookmark = datetime.fromtimestamp(ts_int)
                                yield self._convert_date_fields(
                                    {"channel_id": channel_id, **message},
                                    self.date_fields,
                                )
                except FatalAPIError as ex:
                    if "not_in_channel" in str(ex):
                        LOGGER.warning("Attempted to get messages for channel %s that bot is not in", channel_id)
                    else:
                        raise

                self._set_channel_bookmark(channel_id, min_bookmark.strftime(DATETIME_FORMAT))
                date_window_start = date_window_end
                date_window_end = min(date_window_start + timedelta(days=date_window_size), end)

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {
            "channel_id": record["channel_id"],
            "thread_ts": record.get("thread_ts"),
        }


class ThreadsStream(SlackRESTStream):
    name = "threads"
    primary_keys = ["channel_id", "ts", "thread_ts"]
    replication_method = "FULL_TABLE"
    parent_stream_type = ConversationHistoryStream
    date_fields = ["ts", "last_read"]

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        if not context:
            return

        channel_id = context["channel_id"]
        thread_ts = context["thread_ts"]
        start, end = self._get_absolute_date_range(self.config.get("start_date"))
        try:
            pages = self._request_paginated_json(
                "conversations.replies",
                params={
                    "channel": channel_id,
                    "ts": thread_ts,
                    "inclusive": "true",
                    "oldest": int(start.timestamp()),
                    "latest": int(end.timestamp()),
                    "limit": 200,
                },
                context=context,
            )
            for page in pages:
                raw_threads = transform_json(
                    stream=self.name,
                    data=page.get("messages", []),
                    date_fields=self.date_fields,
                    channel_id=channel_id,
                )
                for message in raw_threads:
                    yield self._convert_date_fields(message, self.date_fields)
        except Exception:
            LOGGER.exception("Failed to sync thread %s in channel %s", thread_ts, channel_id)


class UsersStream(SlackRESTStream):
    name = "users"
    primary_keys = ["id"]
    replication_key = "updated"
    date_fields = ["updated"]

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        bookmark = self.stream_state.get("replication_key_value") or self.config.get("start_date")
        for page in self._request_paginated_json("users.list", params={"limit": 200}):
            users = transform_json(
                stream=self.name,
                data=page.get("members", []),
                date_fields=self.date_fields,
            )
            for user in users:
                self._convert_date_fields(user, self.date_fields)
                if user.get("updated", "") > bookmark:
                    yield user


class UserGroupsStream(SlackRESTStream):
    name = "user_groups"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        pages = self._request_paginated_json(
            "usergroups.list",
            params={"include_count": "true", "include_disabled": "true", "include_users": "true"},
        )
        for page in pages:
            for usergroup in page.get("usergroups", []):
                yield usergroup


class TeamsStream(SlackRESTStream):
    name = "teams"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        payload = self._request_json("team.info")
        team = payload.get("team")
        if team:
            yield team


class FilesStream(SlackRESTStream):
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
        date_window_end = min(start + timedelta(days=date_window_size), end)
        min_bookmark = start
        max_bookmark = start

        while date_window_start < end:
            pages = self._request_paginated_json(
                "files.list",
                params={
                    "from_ts": int(date_window_start.timestamp()),
                    "to_ts": int(date_window_end.timestamp()),
                    "limit": 200,
                },
            )
            for page in pages:
                for file_record in page.get("files", []):
                    ts_int = int(file_record.get("timestamp", 0))
                    if ts_int >= start.timestamp():
                        record_dt = datetime.fromtimestamp(ts_int, tz=timezone.utc)
                        if record_dt > max_bookmark.replace(tzinfo=timezone.utc):
                            max_bookmark = datetime.fromtimestamp(ts_int)
                        elif record_dt < min_bookmark.replace(tzinfo=timezone.utc):
                            min_bookmark = datetime.fromtimestamp(ts_int)
                        yield self._convert_date_fields(file_record, self.date_fields)

            self._set_file_bookmark(min_bookmark.strftime(DATETIME_FORMAT))
            date_window_start = date_window_end
            date_window_end = min(date_window_start + timedelta(days=date_window_size), end)


class RemoteFilesStream(SlackRESTStream):
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
        date_window_end = min(start + timedelta(days=date_window_size), end)
        min_bookmark = start
        max_bookmark = start

        while date_window_start < end:
            pages = self._request_paginated_json(
                "files.remote.list",
                params={
                    "from_ts": int(date_window_start.timestamp()),
                    "to_ts": int(date_window_end.timestamp()),
                    "limit": 200,
                },
            )
            for page in pages:
                remote_files = transform_json(
                    stream=self.name,
                    data=page.get("files", []),
                    date_fields=self.date_fields,
                )
                for file_record in remote_files:
                    ts_int = int(file_record.get("timestamp", "0").partition(".")[0] or 0)
                    if ts_int >= start.timestamp():
                        record_dt = datetime.fromtimestamp(ts_int, tz=timezone.utc)
                        if record_dt > max_bookmark.replace(tzinfo=timezone.utc):
                            max_bookmark = datetime.fromtimestamp(ts_int)
                        elif record_dt < min_bookmark.replace(tzinfo=timezone.utc):
                            min_bookmark = datetime.fromtimestamp(ts_int)
                        yield self._convert_date_fields(file_record, self.date_fields)

            self._set_remote_file_bookmark(min_bookmark.strftime(DATETIME_FORMAT))
            date_window_start = date_window_end
            date_window_end = min(date_window_start + timedelta(days=date_window_size), end)


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
