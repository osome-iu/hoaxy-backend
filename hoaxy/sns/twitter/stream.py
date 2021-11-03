# -*- coding: utf-8 -*-
"""Use the steam API to track tweets."""
#
# rewritten by Chengcheng Shao <sccotte@gmail.com>
# original author: Giovanni Luca Ciampaglia <gciampag@indiana.edu>

# TODO: set request timeout to 90 seconds to manage stalls and implement
# backoff strategies as documented here:
# https://dev.twitter.com/streaming/overview/connecting (see Stalls and
# Reconnecting)
#
# and also see:
# http://docs.python-requests.org/en/latest/user/quickstart/#timeouts

try:
    import simplejson as json
except ImportError:
    import json
import logging
import socket
import time

import requests
from requests_oauthlib import OAuth1
from hoaxy.sns.twitter.handlers import QueueHandler

logger = logging.getLogger(__name__)
# API_URL = 'https://stream.twitter.com/1.1/statuses/filter.json'
RULES_API_URL = "https://api.twitter.com/2/tweets/search/stream/rules"
STREAM_API_URL = "https://api.twitter.com/2/tweets/search/stream"

# all times are in seconds. Start with time, then depending on whether
# kind is linear or exponential either add step or multiply by factor.
# Raise a critical error after waiting max.
BACKOFF_PARAMS = {
    'tcp': {
        'time': 0,
        'kind': 'linear',
        'step': .250,
        'max': 16
    },
    'http': {
        'time': 5,
        'kind': 'exponential',
        'factor': 2,
        'max': 320
    },
    'http_420': {
        'time': 60,
        'kind': 'exponential',
        'factor': 2,
        'max': 600
    }
}

# def _append(self):
#     if self.outpath == '-':
#         self.outfp = sys.stdout
#     elif self.outpath.endswith("gz"):
#         self.outfp = gzip.GzipFile(filename=self.outpath, mode='a')
#     else:
#         self.outfp = open(self.outpath, 'a')


class TwitterStreamError(Exception):
    """Exception of TwitterStream"""


class TwitterStream():
    """Twitter stream implementation in threading way."""

    def __init__(self, credentials, output_fields, handlers, params, window_size=1000):
        """Constructor for TwitterStream.

        Parameters
        ----------
        credentials : dict
            The twitter app authentication keys.
        handlers : list
            A list of handlers to  handle tweet. Handlers should
            process fast enough to not block streaming.
        params : dict
            Parameters for Twitter streaming API.
        window_size : int
            The size of window, do logging.
        """
        self.credentials = credentials
        self.output_fields = output_fields
        self.handlers = handlers
        self.window_size = window_size
        self.params = params
        self.client = requests.session()
        self._stall_timeout = 90
        self._backoff_sleep = None  # if not None, we are backing off
        self._backoff_strategy = None
        self._conn_timeout_sleep = .5
        self._counter = 0  # overall counter of processed tweets
        self._backoff_params = BACKOFF_PARAMS
        logger.info("handlers: %s", self.handlers)
        logger.info("Twitter API parameters: %s", self.params)

    def _authenticate(self):
        """Authenticate and return a requests client object."""
    #     crd = self.credentials
    #     oauth = OAuth1(
    #         client_key=crd['consumer_key'],
    #         client_secret=crd['consumer_secret'],
    #         resource_owner_key=crd['access_token'],
    #         resource_owner_secret=crd['access_token_secret'],
    #         signature_type='auth_header')
    #     self.client = requests.session()
    #     self.client.auth = oauth

        crd = self.credentials
        bearer_token = crd['bearer_token']
        header = {"Authorization": "Bearer {}".format(bearer_token)}
        return header

    def _backoff(self, strategy):
        """Backoff strategy.

        See https://dev.twitter.com/streaming/overview/connecting
        (Stalls and Reconnecting)

        A strategy defines a set of parameters for the backoff, including
        the initial time, the way it increases the sleep period (linear or
        exponential), and a maximum time after which it's better to just
        raise a critical error.
        """
        try:
            params = self._backoff_params[strategy]
        except KeyError:
            raise ValueError("unknown strategy: {}".format(strategy))
        if self._backoff_sleep is None or self._backoff_strategy != strategy:
            # start with initial time if first backoff or if strategy has
            # changed
            self._backoff_sleep = params['time']
            self._backoff_strategy = strategy
        else:
            # continue with previous strategy
            if self._backoff_sleep >= params['max']:
                logger.error(
                    "Reached maximum backoff time. Raising critical error!")
                raise TwitterStreamError()
            if params['kind'] == 'linear':
                self._backoff_sleep += params['step']
            else:
                self._backoff_sleep *= params['factor']
            logger.warning("Sleeping {:.2f}s as part of {} backoff.".format(
                self._backoff_sleep, params['kind']))
        time.sleep(self._backoff_sleep)

    def _reset_backoff(self):
        """Reset backoff."""
        self._backoff_sleep = None
        self._backoff_strategy = None

    def process_one_line(self, line):
        """process one line received from Twitter API.

        Return True if everything goes well, else False.
        """
        # empty line, return
        if not line:
            return True
        try:
            jd = json.loads(line)
        except json.JSONDecodeError as err:
            logger.error('Json loads error: %s, raw data: %s', err, line)
            return False
        # in version 2 api it is in_reply_to_status_id_str (need to be changed)
        if not ('in_reply_to_status_id' in jd and 'user' in jd and 'id' in jd):
            logger.error('Not status tweet: %s', jd)
            return False
        self._counter += 1
        if self._counter % self.window_size == 0:
            logger.info('TwitterStreamer received %s tweets', self._counter)
        for handler in self.handlers:
            if isinstance(handler, QueueHandler) and not handler.is_alive():
                raise SystemExit('Consumer thread dead')
            handler.process_one(jd)
        return True

    def get_rules(self):
        """This method checks if rules are exist for the api v2 stream,
        if exists rules it return object with array of rules ids"""

        rules = {}
        try:
            response = self.client.get(RULES_API_URL, headers=self._authenticate())
            # This condition checks the count of rules exist from the twitter api if 0 then no rules exist.
            if response.json()['meta']['result_count'] > 0:
                ids = []
                for value in response.json()['data']:
                    ids.append(value["id"])
                    rules["ids"] = ids
            return rules
        except requests.HTTPError as err:
            raise err

    def add_rules_util(self):
        try:
            response = self.client.post(RULES_API_URL, json=self.params, headers=self._authenticate())
            created_rules = response.json()['meta']['summary']['created']
            not_created_rules = response.json()['meta']['summary']['not_created']
            if created_rules != 0 and not_created_rules == 0:
                logger.warning("The rules has been add to the stream successfully")
            else:
                logger.warning("One rule or more has not been add due to duplicated value")
        except requests.HTTPError as err:
            raise err

    def add_rules(self):
        """This method works on deleting the rules if exist and add new rules"""
        self.get_rules()
        if self.get_rules() != {}: # If the rules object returned in not empty and has rules then delete the rules.
            delete = {'delete': self.get_rules()}
            try:
                response = self.client.post(RULES_API_URL, json=delete, headers=self._authenticate())
                not_deleted = response.json()['meta']['summary']['not_deleted']
                if not_deleted == 0:
                    logger.warning("The rules has been deleted successfully")
                else:
                    logger.warning("The rules has not been deleted successfully, something went wrong")
            except requests.HTTPError as err:
                raise err
            finally: # Finally block to trigger the add rules request after the deletion complete successfully
                self.add_rules_util()
        else:  # If the get rules object empty then directly start the add rules process.
            self.add_rules_util()

    def stream(self):
        """The main function to handle twitter stream."""
        logger.info("Started streaming.")
        while True:
            try:
                self.add_rules()
                # get the existing rules
                #  if there is no existing rules, add new rules
                # if there is existing rules, delete them all
                # add the new rules
                # self._authenticate()
                parameters = "tweet.fields=" + self.output_fields
                resp = self.client.post(
                    STREAM_API_URL,
                    # data=self.params,
                    params=parameters,
                    headers=self._authenticate(),
                    stream=True,
                    timeout=self._stall_timeout)
                data_lines = 0  # includes keepalives
                # line is unicode
                for line in resp.iter_lines():
                    self.process_one_line(line)
                    data_lines += 1
                    if data_lines >= 8:
                        # reset backoff status if received at least 8 data
                        # lines (including keep-alive newlines). Stream
                        # seems to send at least 8 keepalives, regardless
                        # of whether authentication was successful or not.
                        logger.debug("Reset backoff")
                        self._reset_backoff()
                        data_lines = 0
                logger.warning("Backing off..")
                self._backoff('tcp')
            except requests.exceptions.ConnectTimeout:
                # wait just a (small) fixed amount of time and try to
                # reconnect.
                msg = "Timeout, retrying in %s.."
                logger.warning(msg, self._conn_timeout_sleep)
                time.sleep(self._conn_timeout_sleep)
            except requests.Timeout:
                # catching requests.Timeout instead of requests.ReadTimeout
                # because we are setting a timeout parameter in the POST
                msg = "Server did not send any data for %ss, backing off"
                logger.warning(msg, self._stall_timeout)
                self._backoff('tcp')
            except requests.ConnectionError:
                logger.warning("Reconnecting to stream endpoint...")
                self._backoff('tcp')
            except requests.HTTPError as err:
                if err.response.status_code == 420:
                    msg = "Got HTTP 420 Error. Backing off.."
                    logger.warning(msg)
                    self._backoff("http_420")
                else:
                    msg = "Got HTTP Error. Backing off.."
                    logger.warning(msg)
                    self._backoff("http")
            except socket.error as err:
                logger.warning('Got socket error: %s, reconnecting!', err)
                self._backoff('tcp')
            finally:
                # close the request connection
                logger.info('Request connection closed!')
                resp.close()
