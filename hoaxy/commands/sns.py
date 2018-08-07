# -*- coding: utf-8 -*-
"""Hoaxy subcommand SNS implementation.

Collect data from social networks. Right now only twitter platform is
implemented.
"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

import gzip
import logging
import Queue
import smtplib
import time

import simplejson as json

from hoaxy.commands import HoaxyCommand
from hoaxy.database import Session
from hoaxy.database.functions import get_platform_id, get_site_tuples
from hoaxy.database.models import N_PLATFORM_TWITTER
from hoaxy.sns.twitter.handlers import QueueHandler
from hoaxy.sns.twitter.parsers import BulkParser, QueueParser
from hoaxy.sns.twitter.stream import TwitterStream
from hoaxy.utils import get_track_keywords
from hoaxy.utils.log import configure_logging

logger = logging.getLogger(__name__)


class SNS(HoaxyCommand):
    """
usage:
  hoaxy sns --twitter-streaming
            [(--mail-from=<f> --mail-to=<t>) --mail-server=<s>]
  hoaxy sns --twitter-load-gzfile --gzfile=<gf>
  hoaxy sns -h | --help

Track posted messages in social networks. Right now only twitter platform is
implemented.

--twitter-streaming     Start twitter streaming.
--twitter-load-gzfile   Load tweets store as gzip file. Each line is a tweet.
--mail-server=<s>       SMTP server host address. When process exits
                          unexceptionally, hoaxy send you email.
                          [default: localhost]
--mail-from=<f>         From user address, when sending email.
--mail-to=<t>           To user address, when sending email.
--gzfile=<gf>           The name of the gzip file.
-h --help               Show help.

Since twitter streaming is a long running process. It is good to monitor this
process, especially when process exited because of exceptions. We provide
basic mailing options, when the process exits, a notification should be send
to your mail address.

Examples:

  1. Track twitter stream
  hoaxy sns --twitter-streaming
            || --mail-from=root@s.indiana.edu --mail-to=hoaxy@s.edu
    """
    name = 'sns'
    short_description = 'Online social network services management'

    @classmethod
    def twitter_stream(cls, session, args, max_retries=5, retry_stall=60):
        """Twitter streaming process."""
        sites = get_site_tuples(session)
        keywords = get_track_keywords(sites)
        platform_id = get_platform_id(session, name=N_PLATFORM_TWITTER)
        session.close()
        w_size = cls.conf['window_size']
        c = cls.conf['sns']['twitter']['app_credentials']
        snut = cls.conf['sns']['twitter']['save_none_url_tweet']

        retries = 0
        q = Queue.Queue()
        consumer = QueueParser(
            q, platform_id, w_size, save_none_url_tweet=snut)
        qhandler = QueueHandler(q)
        consumer.start()
        stall_time = retry_stall

        while True:
            try:
                streamer = TwitterStream(
                    c, [qhandler], dict(track=keywords), w_size)
                streamer.stream()
            except Exception as e:
                logger.exception(e)
                time.sleep(stall_time)
                if streamer._counter > 100:
                    # reset retry counter and stall time
                    retries = 0
                    stall_time = retry_stall
                else:
                    # increase retry counter and stall time
                    retries += 1
                    stall_time = 2 * stall_time
                    if retries >= max_retries:
                        logger.error('Reached max retries!')
                        break
            except (KeyboardInterrupt, SystemExit):
                break
        consumer.stop()
        s = args['--mail-server']
        f = args['--mail-from']
        t = args['--mail-to']

        if s and f and t:
            logger.info('server %r, from %r, to %r', s, f, t)
            try:
                server = smtplib.SMTP(s)
                msg = 'Twitter streaming is stopped!'
                server.sendmail(f, t, msg)
            except Exception as e:
                logger.error(e)
        logger.info('Exit')

    @classmethod
    def twitter_load_gzfile(cls, gzfile, session):
        snut = cls.conf['sns']['twitter']['save_none_url_tweet']
        platform_id = get_platform_id(session, name=N_PLATFORM_TWITTER)
        parser = BulkParser(platform_id=platform_id, save_none_url_tweet=snut)
        with gzip.open(gzfile, 'r') as f:
            g_urls_map = dict()
            g_uusers_set = set()
            g_edges_set = set()
            i = 0
            for line in f:
                try:
                    jd = json.loads(line, encoding='utf-8')
                except Exception as e:
                    logger.error(e)
                    continue
                parser.parse_new_one(
                    jd,
                    session,
                    g_urls_map=g_urls_map,
                    g_uusers_set=g_uusers_set,
                    g_edges_set=g_edges_set)
                i = i + 1
                if i%10000 == 0:
                    logger.info('Counter %s', i)
                    parser.save_bulk(session, g_uusers_set, g_edges_set)
                    g_urls_map = dict()
                    g_uusers_set = set()
                    g_edges_set = set()

    @classmethod
    def run(cls, args):
        """Overriding method as the entry point of this command."""
        session = Session(expire_on_commit=False)
        if args['--twitter-streaming'] is True:
            configure_logging(
                'twitter.streaming', console_level=args['--console-log-level'])
            cls.twitter_stream(session, args)

        if args['--twitter-load-gzfile'] is True:
            configure_logging(
                'twitter.load-gzfile', console_level=args['--console-log-level'])
            cls.twitter_load_gzfile(args['--gzfile'], session)
