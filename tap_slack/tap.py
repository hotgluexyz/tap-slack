import json

import singer
from hotglue_singer_sdk import Tap
from hotglue_singer_sdk import typing as th
from slack import WebClient

from tap_slack import auto_join, discover, sync
from tap_slack.auth import SlackOAuthAuthenticator
from tap_slack.client import SlackClient

SLACK_OAUTH_ENDPOINT = "https://slack.com/api/oauth.v2.access"


class TapSlack(Tap):
    name = "tap-slack"

    config_jsonschema = th.PropertiesList(
        th.Property("client_id", th.StringType),
        th.Property("client_secret", th.StringType),
        th.Property("access_token", th.StringType),
        th.Property("start_date", th.DateTimeType, required=True),
        th.Property("date_window_size", th.StringType),
        th.Property("lookback_window", th.StringType),
        th.Property("channels", th.ArrayType(th.StringType)),
        th.Property("private_channels", th.StringType),
        th.Property("exclude_archived", th.StringType),
        th.Property("join_public_channels", th.StringType),
        th.Property("refresh_token", th.StringType),
        th.Property("expires_in", th.IntegerType),
    ).to_dict()

    @classmethod
    def access_token_support(cls, connector=None):
        return SlackOAuthAuthenticator, SLACK_OAUTH_ENDPOINT

    def discover_streams(self):
        return []
    
    def _resolve_access_token(self, config):
        refresh_token = config.get("refresh_token")
        refresh_token_value = getattr(refresh_token, "contents", refresh_token)

        if refresh_token_value:
            self.__class__.update_access_token(
                SlackOAuthAuthenticator,
                SLACK_OAUTH_ENDPOINT,
                self,
            )
            updated_token = self.config.get("access_token")
            return updated_token

        token = config.get("access_token")
        if not token:
            raise ValueError("Either access_token or OAuth refresh credentials are required.")
        return token

    def _build_client(self):
        token = self._resolve_access_token(self.config)
        config = dict(self.config)
        webclient = WebClient(token=token)
        return SlackClient(webclient=webclient, config=config), config

    def run_discovery(self):
        client, _ = self._build_client()
        discover(client=client)

    def run_sync(self, catalog=None, state=None):
        client, config = self._build_client()
        parsed_catalog = singer.catalog.Catalog.load(catalog)
        parsed_state = json.load(open(state))

        if config.get("join_public_channels", "false") == "true":
            auto_join(client=client, config=config)

        sync(
            client=client,
            config=config,
            catalog=parsed_catalog,
            state=parsed_state,
        )
