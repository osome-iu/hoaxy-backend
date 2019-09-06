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
from requests_oauthlib import OAuth1
import logging
import requests
import socket
import time

logger = logging.getLogger(__name__)
API_URL = 'https://stream.twitter.com/1.1/statuses/filter.json'

# all times are in seconds. Start with time, then depending on whether
# kind is linear or exponential either add step or multiply by factor.
# Raise a fatal error after waiting max.
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
    pass


class TwitterStream(object):
    """Twitter stream implementation in threading way."""

    def __init__(self, credentials, handlers, params, window_size=1000):
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
        self.handlers = handlers
        self.window_size = window_size
        self.params = params
        self._stall_timeout = 90
        self._backoff_sleep = None  # if not None, we are backing off
        self._backoff_strategy = None
        self._conn_timeout_sleep = .5
        self._counter = 0  # overall counter of processed tweets
        self._backoff_params = BACKOFF_PARAMS
        logger.info("handlers: {}".format(self.handlers))
        logger.info("Twitter API parameters: {}".format(self.params))

    def _authenticate(self):
        """Authenticate and return a requests client object."""
        c = self.credentials
        oauth = OAuth1(
            client_key=c['consumer_key'],
            client_secret=c['consumer_secret'],
            resource_owner_key=c['access_token'],
            resource_owner_secret=c['access_token_secret'],
            signature_type='auth_header')
        self.client = requests.session()
        self.client.auth = oauth

    def _backoff(self, strategy):
        """Backoff strategy.

        See https://dev.twitter.com/streaming/overview/connecting
        (Stalls and Reconnecting)

        A strategy defines a set of parameters for the backoff, including
        the initial time, the way it increases the sleep period (linear or
        exponential), and a maximum time after which it's better to just
        raise a fatal error.
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
                    "Reached maximum backoff time. Raising fatal error!")
                raise TwitterStreamError()
            if params['kind'] == 'linear':
                self._backoff_sleep += params['step']
            else:
                self._backoff_sleep *= params['factor']
            logger.warn("Sleeping {:.2f}s as part of {} backoff.".format(
                self._backoff_sleep, params['kind']))
        time.sleep(self._backoff_sleep)

    def _reset_backoff(self):
        """Reset backoff."""
        self._backoff_sleep = None
        self._backoff_strategy = None

    def stream(self):
        """The main function to handle twitter stream."""
        logger.info("Started streaming.")
        try:
            while True:
                try:
                    self._authenticate()
                    resp = self.client.post(
                        API_URL,
                        data=self.params,
                        stream=True,
                        timeout=self._stall_timeout)
                    data_lines = 0  # includes keepalives
                    # line is unicode
                    for line in resp.iter_lines():
                        data_lines += 1
                        if line:
                            try:
                                jd = json.loads(line)
                            except Exception as e:
                                msg = 'Json loads error: %s, raw data: %s'
                                logger.error(msg, e, line)
                                continue
                            if not ('in_reply_to_status_id' in jd and
                                    'user' in jd and 'text' in jd):
                                logger.error('Not status tweet: %s', jd)
                                continue
                            self._counter += 1
                            if self._counter % self.window_size == 0:
                                logger.info("{} tweets.".format(self._counter))
                            for handler in self.handlers:
                                handler.process_one(jd)
                        if data_lines >= 8:
                            # reset backoff status if received at least 8 data
                            # lines (including keep-alive newlines). Stream
                            # seems to send at least 8 keepalives, regardless
                            # of whether authentication was successful or not.
                            logger.debug("Reset backoff")
                            self._reset_backoff()
                            data_lines = 0
                    logger.warn("Backing off..")
                    self._backoff('tcp')
                except requests.exceptions.ConnectTimeout:
                    # wait just a (small) fixed amount of time and try to
                    # reconnect.
                    msg = "Timeout while trying to connect to server. " +\
                        "Retrying in {}s.."
                    logger.warn(msg.format(self._conn_timeout_sleep))
                    time.sleep(self._conn_timeout_sleep)
                except requests.Timeout:
                    # catching requests.Timeout instead of requests.ReadTimeout
                    # because we are setting a timeout parameter in the POST
                    msg = "Server did not send any data for {}s. " +\
                        "Backing off.."
                    logger.warn(msg.format(self._stall_timeout))
                    self._backoff('tcp')
                except requests.ConnectionError:
                    logger.warn("Reconnecting to stream endpoint...")
                    self._backoff('tcp')
                except socket.error as e:
                    msg = "Socket error {}: {}. " +\
                        "Reconnecting to stream endpoint..."
                    logger.warn(msg.format(e.errno, e.message))
                    self._backoff('tcp')
                except requests.HTTPError as e:
                    if e.response.status_code == 420:
                        msg = "Got HTTP 420 Error. Backing off.."
                        logger.warn(msg)
                        self._backoff("http_420")
                    else:
                        msg = "Got HTTP Error. Backing off.."
                        logger.warn(msg)
                        self._backoff("http")
                except KeyboardInterrupt:
                    logger.info("got ^C from user. Exit.")
                    raise
        finally:
            # catch any fatal error (including TwitterStreamError we raise if
            # backoff reaches maximum sleep time)
            resp.close()
            try:
                for h in self.handlers:
                    h.close()
            except Exception as e:
                logger.error(e)
