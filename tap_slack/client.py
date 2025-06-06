"""
Client wrapping the python Slack SDK.
Handles retry/backoff logic using the Singer framework annotations.
"""
import time

import backoff
import logging
import singer
from slack.errors import SlackApiError

LOGGER = singer.get_logger()

logging.getLogger('backoff').setLevel(logging.CRITICAL)

def log_backoff_attempt(details):
    LOGGER.info("ConnectionFailure detected, triggering backoff: %d try", details.get("tries"))

class SlackClient(object):

    def __init__(self, webclient, config):
        self.webclient = webclient
        self.config = config

    def wait(err=None):
        if isinstance(err, SlackApiError):
            if err.response.data.get("error", "") == "ratelimited":
                delay = int(err.response.headers.get("Retry-After", "0"))
            else:
                raise err
            time.sleep(delay)

    @backoff.on_exception(backoff.constant,
                          (SlackApiError, TimeoutError),
                          max_tries=2,
                          jitter=None,
                          giveup=wait,
                          interval=0,
                          on_backoff=log_backoff_attempt)
    def get_all_channels(self, types, exclude_archived):

        return self.webclient.conversations_list(
            exclude_archived=exclude_archived,
            types=types)

    @backoff.on_exception(backoff.constant,
                          (SlackApiError, TimeoutError),
                          max_tries=2,
                          jitter=None,
                          giveup=wait,
                          interval=0,
                          on_backoff=log_backoff_attempt)
    def get_channel(self, include_num_members, channel=None):
        page = self.webclient.conversations_info(channel=channel,
                                                 include_num_members=include_num_members)
        yield page.get('channel')

    @backoff.on_exception(backoff.constant,
                          (SlackApiError, TimeoutError),
                          max_tries=2,
                          jitter=None,
                          giveup=wait,
                          interval=0,
                          on_backoff=log_backoff_attempt)
    def get_channel_members(self, channel):
        try:
            members_cursor = self.webclient.conversations_members(channel=channel)
        except SlackApiError as err:
            if err.response.data.get("error", "") == "fetch_members_failed":
                LOGGER.warning('Failed to fetch members for channel: {}'
                               .format(channel))
                members_cursor = []
            else:
                raise err

        return members_cursor

    @backoff.on_exception(backoff.constant,
                          (SlackApiError, TimeoutError),
                          max_tries=2,
                          jitter=None,
                          giveup=wait,
                          interval=0,
                          on_backoff=log_backoff_attempt)
    def get_messages(self, channel, oldest, latest):
        try:
            LOGGER.info(f"Getting messages for channel: {channel}, oldest: {oldest}, latest: {latest}")
            messages = self.webclient \
                .conversations_history(channel=channel,
                                       oldest=oldest,
                                       latest=latest)
        except SlackApiError as err:
            if err.response.data.get("error", "") == "not_in_channel":
                # The tap config might dictate that archived channels should
                # be processed, but if the slackbot was not made a member of
                # those channels prior to archiving attempting to get the
                # messages will throw an error
                LOGGER.warning(
                    'Attempted to get messages for channel: {} that '
                    'slackbot is not in'.format(
                        channel
                    ))
                messages = None
            else:
                raise err

        LOGGER.info(f"Got messages, processing now {len(messages.get('messages', []))}")
        return messages

    @backoff.on_exception(backoff.constant,
                          (SlackApiError, TimeoutError),
                          max_tries=2,
                          jitter=None,
                          giveup=wait,
                          interval=0,
                          on_backoff=log_backoff_attempt)
    def get_thread(self, channel, ts, inclusive, oldest, latest):
        return self.webclient.conversations_replies(channel=channel,
                                                    ts=ts,
                                                    inclusive=inclusive,
                                                    oldest=oldest,
                                                    latest=latest)

    @backoff.on_exception(backoff.constant,
                          (SlackApiError, TimeoutError),
                          max_tries=2,
                          jitter=None,
                          giveup=wait,
                          interval=0,
                          on_backoff=log_backoff_attempt)
    def get_users(self, limit):
        return self.webclient.users_list(limit=limit)

    @backoff.on_exception(backoff.constant,
                          (SlackApiError, TimeoutError),
                          max_tries=2,
                          jitter=None,
                          giveup=wait,
                          interval=0,
                          on_backoff=log_backoff_attempt)
    def get_user_groups(self, include_count, include_disabled, include_user):
        return self.webclient.usergroups_list(include_count=include_count,
                                              include_disabled=include_disabled,
                                              include_user=include_user)

    @backoff.on_exception(backoff.constant,
                          (SlackApiError, TimeoutError),
                          max_tries=2,
                          jitter=None,
                          giveup=wait,
                          interval=0,
                          on_backoff=log_backoff_attempt)
    def get_teams(self):
        return self.webclient.team_info()

    @backoff.on_exception(backoff.constant,
                          (SlackApiError, TimeoutError),
                          max_tries=2,
                          jitter=None,
                          giveup=wait,
                          interval=0,
                          on_backoff=log_backoff_attempt)
    def get_files(self, from_ts, to_ts):
        return self.webclient.files_list(from_ts=from_ts, to_ts=to_ts)

    @backoff.on_exception(backoff.constant,
                          (SlackApiError, TimeoutError),
                          max_tries=2,
                          jitter=None,
                          giveup=wait,
                          interval=0,
                          on_backoff=log_backoff_attempt)
    def get_remote_files(self, from_ts, to_ts):
        return self.webclient.files_remote_list(from_ts=from_ts, to_ts=to_ts)

    @backoff.on_exception(backoff.constant,
                          (SlackApiError, TimeoutError),
                          max_tries=2,
                          jitter=None,
                          giveup=wait,
                          interval=0,
                          on_backoff=log_backoff_attempt)
    def join_channel(self, channel):
        return self.webclient.conversations_join(channel=channel)
