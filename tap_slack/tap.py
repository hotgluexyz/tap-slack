import logging

from hotglue_singer_sdk import Tap, Stream
from hotglue_singer_sdk import typing as th

from tap_slack.auth import SlackOAuthAuthenticator

LOGGER = logging.getLogger(__name__)


class TapSlack(Tap):
    name = "tap-slack"

    config_jsonschema = th.PropertiesList(
        th.Property("access_token", th.StringType, required=True),
        th.Property("start_date", th.DateTimeType, required=True),
        th.Property("date_window_size", th.StringType),
        th.Property("lookback_window", th.StringType),
        th.Property("channels", th.ArrayType(th.StringType)),
        th.Property("private_channels", th.StringType),
        th.Property("exclude_archived", th.StringType),
        th.Property("join_public_channels", th.StringType),
        th.Property("client_id", th.StringType),
        th.Property("client_secret", th.StringType),
        th.Property("refresh_token", th.StringType),
    ).to_dict()

    @classmethod
    def access_token_support(cls, connector=None):
        return (SlackOAuthAuthenticator, "https://slack.com/api/oauth.v2.access")

    def sync_all(self) -> None:
        if self.config.get("join_public_channels", "false") == "true":
            self._auto_join()
        super().sync_all()

    def _auto_join(self) -> None:
        from tap_slack.streams import ConversationsStream

        client = ConversationsStream(tap=self)
        if "channels" in self.config:
            for channel_id in self.config.get("channels"):
                join_response = client.join_channel(channel_id)
                if not join_response.get("ok", False):
                    error = join_response.get("error", "Unspecified Error")
                    LOGGER.error("Error joining %s, Reason: %s", channel_id, error)
                    raise Exception(f"{channel_id}: {error}")
        else:
            for conversation in client.iter_channels():
                conversation_id = conversation.get("id")
                conversation_name = conversation.get("name")
                join_response = client.join_channel(conversation_id)
                if not join_response.get("ok", False):
                    error = join_response.get("error", "Unspecified Error")
                    LOGGER.error("Error joining %s, Reason: %s", conversation_name, error)
                    raise Exception(f"{conversation_name}: {error}")

    def discover_streams(self) -> list[Stream]:
        from tap_slack.streams import STREAM_TYPES
        return [cls(tap=self) for cls in STREAM_TYPES]
