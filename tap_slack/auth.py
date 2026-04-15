from hotglue_singer_sdk.authenticators import OAuthAuthenticator


class SlackOAuthAuthenticator(OAuthAuthenticator):
    """OAuth authenticator for Slack using refresh_token grant (token rotation)."""

    @property
    def oauth_request_payload(self) -> dict:
        return {
            "client_id": self._tap.config.get("client_id"),
            "client_secret": self._tap.config.get("client_secret"),
            "refresh_token": self._tap.config.get("refresh_token"),
            "grant_type": "refresh_token",
        }
