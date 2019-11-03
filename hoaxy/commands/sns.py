# -*- coding: utf-8 -*-
"""Hoaxy subcommand SNS implementation.

Collect data from social networks. Right now only twitter platform is
implemented.
"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

import logging
import Queue
import sys

import simplejson as json

from hoaxy.commands import HoaxyCommand
from hoaxy.database import Session
from hoaxy.database.functions import get_platform_id, get_site_tuples
from hoaxy.database.models import N_PLATFORM_TWITTER
from hoaxy.sns.twitter.handlers import QueueHandler
from hoaxy.sns.twitter.parsers import Parser
from hoaxy.sns.twitter.stream import TwitterStream
from hoaxy.utils import get_track_keywords
from hoaxy.utils.log import configure_logging
from schema import Or, Schema, SchemaError, Use
from xopen import xopen

logger = logging.getLogger(__name__)


class SNS(HoaxyCommand):
    """
usage:
  hoaxy sns --twitter-streaming
  hoaxy sns --load-tweets [--strict-on-error] [--number-of-tweets=<nt>]
            <filepath>
  hoaxy sns -h | --help

Track posted messages in social networks. Right now only twitter platform is
implemented.

--twitter-streaming     Start twitter streaming.
--load-tweets           Load local tweets from file, one tweet per line.
--strict-on-error       By default, we would try our best to read and parse
                        lines, ignore possible errors when parsing and continue
                        on the next line. However, If this flag is set, the
                        program will exit (with 1) on any error.
--number-of-tweets=<nt> How many tweets to collect. If not set, the number is
                        not set, the number is unlimited.
<filepath>              File that stores the JSON structured tweets, one tweet
                          per line. Compressed format are supported, which are
                          automatically recognized by their file extension
                          .gz, .bz2 or .xz.
-h --help               Show help.

Since twitter streaming is a long running process. It is good to monitor this
process, especially when process exited because of exceptions. We provide
basic mailing options, when the process exits, a notification should be send
to your mail address.

Examples:

  1. Track twitter stream
  hoaxy sns --twitter-streaming
            || --mail-from=root@s.indiana.edu --mail-to=hoaxy@s.edu

  2. Load local tweets
  hoaxy sns --load-tweets dumped_tweets.json.gz
    """
    name = 'sns'
    short_description = 'Online social network services management'
    args_schema = Schema({
        '--number-of-tweets': Or(None, Use(int)),
        object: object
    })

    @classmethod
    def twitter_stream(cls, session, args):
        """Twitter streaming process."""
        sites = get_site_tuples(session)
        keywords = get_track_keywords(sites)
        session.close()
        window_size = cls.conf['window_size']
        credentials = cls.conf['sns']['twitter']['app_credentials']
        save_none_url_tweet = cls.conf['sns']['twitter']['save_none_url_tweet']

        queue = Queue.Queue()
        consumer = QueueHandler(
            queue,
            bucket_size=window_size,
            parser_kwargs=dict(save_none_url_tweet=save_none_url_tweet))
        consumer.start()
        try:
            streamer = TwitterStream(
                credentials=credentials,
                handlers=[consumer],
                params=dict(track=keywords),
                window_size=window_size)
            streamer.stream()
        except Exception as e:
            logger.exception(e)
        consumer.stop()

        logger.info('Exit')

    @classmethod
    def load_tweets(cls, session, args, bucket_size=10000):
        parser = Parser()
        ntweets = args['--number-of-tweets']
        strict_on_error = args['--strict-on-error']
        true_counter = 0
        counter = 0
        jds = []
        f = xopen(args['<filepath>'])
        platform_id = get_platform_id(session, N_PLATFORM_TWITTER)
        while True:
            line = f.readline()
            counter += 1
            if line:
                try:
                    jd = json.loads(line)
                    if 'in_reply_to_status_id' in jd and 'user' in jd and\
                            'text' in jd:
                        jds.append(json.loads(line))
                        true_counter += 1
                    else:
                        logger.error('Not a tweet at line %s, raw data %r',
                                     counter, jd)
                        if strict_on_error:
                            sys.exit(1)
                        continue
                except Exception as e:
                    msg = 'JSON loads error at line %s: %r, raw data: %r'
                    logger.error(msg, counter, e, line)
                    if strict_on_error:
                        sys.exit(1)
                    continue
            else:
                logger.error('Empty line at line %s', counter)
            if ntweets is not None and ntweets == true_counter:
                logger.warning('Reaching the number of tweets %s at line %s',
                               ntweets, counter)
                # break the loop
                break
            if true_counter % bucket_size == 0:
                logger.warning('Reading %s lines, %s tweets parsed', counter,
                               true_counter)
                # parsed_results = parser.parse_many(jds, multiprocesses=True)
                parsed_results = [parser.parse_one(jd) for jd in jds]
                dfs = parser.to_dict(parsed_results)
                parser.bulk_save(session, dfs, platform_id)
                jds = []
        if jds:
            logger.warning('Reading %s lines, %s tweets parsed', counter,
                           true_counter)
            parsed_results = parser.parse_many(jds, multiprocesses=True)
            dfs = parser.to_dict(parsed_results)
            parser.bulk_save(session, dfs, platform_id)
            jds = []

    @classmethod
    def run(cls, args):
        """Overriding method as the entry point of this command."""
        try:
            args = cls.args_schema.validate(args)
        except SchemaError as e:
            raise SystemExit(e)
        session = Session(expire_on_commit=False)
        if args['--twitter-streaming'] is True:
            configure_logging('twitter.streaming')
            cls.twitter_stream(session, args)
        elif args['--load-tweets'] is True:
            configure_logging('twitter.load-tweets')
            cls.load_tweets(session, args)
