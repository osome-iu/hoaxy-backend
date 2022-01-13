# -*- coding: utf-8 -*-
"""Hoaxy subcommand SNS implementation.

Collect data from social networks. Right now only twitter platform is
implemented.
"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

import itertools
import logging
import os
import os.path
import signal
import sys
from queue import Queue

import simplejson as json
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from xopen import xopen

from hoaxy import HOAXY_HOME
from hoaxy.commands import HoaxyCommand
from hoaxy.database import Session
from hoaxy.database.functions import get_platform_id, get_site_tuples
from hoaxy.database import hoaxy_connection_pool
from hoaxy.database.models import N_PLATFORM_TWITTER
from hoaxy.sns.twitter.handlers import QueueHandler
from hoaxy.sns.twitter.parsers import Parser
from hoaxy.sns.twitter.stream import TwitterStream
from hoaxy.utils import get_track_keywords
from hoaxy.utils.log import configure_logging
from schema import Or, Schema, SchemaError, Use
from datetime import date
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)


def chunked_iterable(iterable, size):
    """Iterate in fixed-size chunks over an iterable. Return an iterable, each
    element is a tuple with fixed-size=size.
    """
    it = iter(iterable)
    while True:
        chunk = list(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


class SNS(HoaxyCommand):
    """
usage:
  hoaxy sns --twitter-streaming [--dump-dir=<d>]
  hoaxy sns --load-tweets [--strict-on-error --window-size=<w>]
            [--number-of-tweets=<nt>] <filepath>
  hoaxy sns --reparse-db-tweets [--window-size=<w>] (--delete-tables=<tn> ...)
            [--ignore-tables=<tn> ...] (--plain-sql=<q>)
  hoaxy sns --prune-db-tweets
  hoaxy sns -h | --help

Track posted messages in social networks. Right now only TWITTER platform is
implemented. Three sub-commands are implemented.

(1) The --twitter-streaming command is used to track streaming tweets with
specified keywords. As we need to parse tweets and save them into database as
well, we use a multi-threading method with a queue as data (tweets)
communication channel. The main thread is mainly used to handle tweets steaming
and the slave-thread is used to parse and save tweets. We use a bulk insertion
operation to reduce database query overhead. For case of database insertion
errors, the under-saving tweets would be dumped. Also, this command is capable
to handle exceptions when database server is temperally down: cache the
incoming tweets in the queue, if connection recovered, process them; if the
database server is down too long to hold the data, dump all the collected
tweets into file. In the latter case, if connection recovered again, the
dumped tweets would not load automatically into database. The adminstrator
need to check and load them.

--twitter-streaming     Start twitter streaming.
--dump-dir=<d>   The folder to save dumped file when error occurs. The
                        default location is HOAXY_HOME/dumps.

(2) The --load-tweets command is used to load local saved tweets (in a file,
format as one tweet per line).

--load-tweets           Load local tweets from file, one tweet per line.
--strict-on-error       By default, we would try our best to read and parse
                        lines, ignore possible errors when parsing and continue
                        on the next line. However, If this flag is set, the
                        program will exit (with 1) on any error.
-w --window-size=<w>    Operations are executed on a window of tweets.
                        This parameter specify the size of the window.
                        It is also used for --reparse-db-tweets subcommand.
                        [default: 10000]
-n --number-of-tweets=<nt> How many tweets to collect. If not set, the number
                        is not set, the number is unlimited.
<filepath>              File that stores the JSON structured tweets, one tweet
                          per line. Compressed format are supported, which are
                          automatically recognized by their file extension
                          .gz, .bz2 or .xz.

(3) The --reparse-db-tweets command is to used to re-parse tweets in the
database and rebuild other tables. Tweets table are the most important table,
from which most other tables could be rebuilt. This command is essentially only
for developers that has changed the tweet parsing schema (e.g., new data field
to save; new network contruction and etc.). Before the operation, it is highly
recommended that you have tested this command on small set, and probably you
need to backup your database. If you want to keep the re-constructed table
clean, a delete operation must be done on desired tables. We provide
`--delete-tables` to support the deletion operation of four tables: ass_tweet,
ass_tweet_url, ass_tweet_hashtag, and twitter_network_edge. Also, if you
definitely sure that some tables would never be updated, you could
specify `--ignored-tables` to ignore any insert operations to save time.
Often, you have to specify `--delete-tables` if parsing schema are changed
(if there are changes for tables out of the above four tables, you have to
take the delete operation by yourself). You do not need to specify
`--ignore-tables` unless you are definitely sure these tables would not be
updated by the reparse process. By default, `tweet` and `twitter_user` table
are ignored.

--reparse-db            Re-construct everything using the tweets in the
                        database. Backup things before running this command.
-q --plain-sql=<q>      The plain SQL query that select specified tweets that
                        are used to re-construct other tables. The SELECT
                        statment should return a list of tweet.raw_id.
-d --delete-tables=<tn>    Specify tables to delete that associated with the
                        re-parsing tweets. Current supported tables are:
                        ass_tweet, ass_tweet_url, ass_tweet_hashtag,
                        twitter_network_edge. Administrator can take direct SQL
                        operations on other tables.
-i --ignore-tables=<tn>    Specify tables that the re-parsing process should
                        not be updated so that we can ignore.
                        WARNING: this parameter should be compatible with
                        `--delete-tables` so that we never ignore tables that
                        are marked as delete. Availabe values are:
                        twitter_user, url, hashtag, twitter_user_union,
                        twitter_network_edge, ass_tweet_url, ass_tweet_hashtag
                        ass_url_platform.

(4) The --prune-db-tweets is used to delete tweets older than 6 months from the tweet table.
6 months date will calculated
--prune-db-tweets       Delete data older than 6 months from the current date from the
                        tweets table in the database.

-h --help               Show help.

At last, as we know that twitter streaming is a long running process. It is
good to monitor this process, especially when process exited because of
exceptions. We provide basic mailing options, when the process exits,
a notification should be send to your mail address. This option works only if
a SMTP logging handler is setup. Check the logging section in conf.yaml.

Examples:

  1. Track twitter stream
  hoaxy sns --twitter-streaming

  2. Load local tweets
  hoaxy sns --load-tweets dumped_tweets.json.gz

  3. Reparse tweets in database when we change the way of how the network
     is built, where we are sure that tables like twitter_network_edge,
     ass_tweet_url (Here we use short-hand options to save place).
  hoaxy sns --reparse-db-tweets --delete-tables=twitter_network_edge
            || -d url -d ass_tweet_url -i twitter_user -i tweet
            || -q "SELECT raw_id from tweet WHERE id<10000"

  4. Delete data beyond 6 months from the tweet table
   hoaxy sns --prune-db-tweets
    """
    name = 'sns'
    short_description = 'Online social network services management'
    args_schema = Schema({
        '--number-of-tweets': Or(None, Use(int)),
        '--window-size': Or(None, Use(int)),
        object: object
    })

    @classmethod
    def twitter_stream(cls, session, args):
        """Twitter streaming process."""
        # create a dump folder
        if args['--dump-dir'] is not None:
            dump_dir = os.path.expanduser(args['--dump-dir'])
            dump_dir = os.path.abspath(dump_dir)
        else:
            dump_dir = os.path.join(HOAXY_HOME, 'dumps')
        if not os.path.exists(dump_dir):
            try:
                org_umask = os.umask(0)
                os.makedirs(dump_dir, 0o755)
            finally:
                os.umask(org_umask)
        sites = get_site_tuples(session)
        keywords = get_track_keywords(sites)
        session.close()
        window_size = cls.conf['window_size']
        output_fields = cls.conf['sns']['twitter']['output_fields']
        credentials = cls.conf['sns']['twitter']['app_credentials']
        save_none_url_tweet = cls.conf['sns']['twitter']['save_none_url_tweet']
        tw_queue = Queue()
        consumer = QueueHandler(
            tw_queue,
            bucket_size=window_size,
            dump_dir=dump_dir,
            parser_kwargs=dict(save_none_url_tweet=save_none_url_tweet))
        consumer.start()
        logger.debug('Consumer thread started.')

        # KeyboardInterrupt signal handler
        def clean_up(signal_n, c_frame):
            raise KeyboardInterrupt

        signal.signal(signal.SIGINT, clean_up)
        # signal.signal(signal.SIGINT, signal.SIG_DFL)

        try:
            streamer = TwitterStream(
                credentials=credentials,
                output_fields=output_fields,
                handlers=[consumer],
                params=dict(add=keywords),
                window_size=window_size)
            streamer.stream()
            logger.info('Twitter steaming exits.')
        except (KeyboardInterrupt, SystemExit):
            logger.info('KeyboardInterruption recevied, cleaning up ...')
            consumer.stop()
            logger.info('Clean up done, exit!')

    @classmethod
    def load_tweets(cls, session, args):
        """Load tweets from file into database.
        """
        parser = Parser()
        ntweets = args['--number-of-tweets']
        strict_on_error = args['--strict-on-error']
        true_counter = 0
        counter = 0
        jds = []
        f = xopen(args['<filepath>'])
        platform_id = get_platform_id(session, N_PLATFORM_TWITTER)
        bucket_size = args['--window-size']
        try:
            for line in f:
                counter += 1
                if line:
                    try:
                        jd = json.loads(line)
                        if 'referenced_tweets' in jd['data'] and 'users' in jd['includes'] and\
                                'text' in jd['data']:
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
                    logger.warning(
                        'Reaching the number of tweets %s at line %s', ntweets,
                        counter)
                    # break the loop
                    break
                if true_counter % bucket_size == 0:
                    logger.warning('Reading %s lines, %s tweets parsed',
                                   counter, true_counter)
                    parser.bulk_parse_and_save(
                        jds, session, platform_id, multiprocesses=True)
                    jds = []
        except Exception as err:
            logger.exception(err)
            logger.info('Saving successfully read tweets ...')
        if jds:
            parser.bulk_parse_and_save(
                jds, session, platform_id, multiprocesses=True)
            jds = []
            logger.warning('Reading %s lines, %s tweets parsed', counter,
                           true_counter)

    @classmethod
    def _test_table_names(cls, session, args):
        rs = session.execute(
            text("""SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema='public'
                        AND table_type='BASE TABLE'; """))
        hoaxy_table_names = [row[0] for row in rs]
        for ignore_table in args['--ignore-tables']:
            if ignore_table not in hoaxy_table_names:
                raise ValueError(
                    'Table {!r} not exist in hoaxy!'.format(ignore_table))
            if ignore_table in args['--delete-tables']:
                msg = "Table %s is marked with delete operation, it should " +\
                    "be set as ignored table!"
                logger.warning(msg, ignore_table)

    @classmethod
    def reparse_db(cls, session, args):
        """Load tweets from file into database.
        """

        def iter_rows_0(rs):
            """Return iterable for row[0] in rs"""
            for row in rs:
                yield row[0]

        parser = Parser()
        bucket_size = args['--window-size']
        plain_sql = args['--plain-sql']
        delete_tables = args['--delete-tables']
        ignore_tables = args['--ignore-tables']
        counter = 0
        table_deletes_sql = dict(
            ass_tweet="""\
            DELETE FROM ass_tweet AS atw
            USING tweet AS tw, UNNEST(:ids) AS t(raw_id)
            WHERE tw.raw_id=t.raw_id AND atw.id=tw.id
            """,
            ass_tweet_url="""\
            DELETE FROM ass_tweet_url AS atu
            USING tweet AS tw, UNNEST(:ids) AS t(raw_id)
            WHERE tw.raw_id=t.raw_id AND atu.id=tw.id
            """,
            ass_tweet_hashtag="""\
            DELETE FROM ass_tweet_hashtag AS ath
            USING tweet AS tw, UNNEST(:ids) AS t(raw_id)
            WHERE tw.raw_id=t.raw_id AND ath.id=tw.id
            """,
            twitter_network_edge="""\
            DELETE FROM twitter_network_edge AS tne
            USING UNNEST(:ids) AS t(raw_id)
            WHERE tne.tweet_raw_id=t.raw_id
            """)

        for tn in delete_tables:
            del_tn = table_deletes_sql.get(tn)
            if del_tn is None:
                raise ValueError('Unsupported deletion of table %s', tn)
        platform_id = get_platform_id(session, N_PLATFORM_TWITTER)
        logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
        rs = session.execute(text(plain_sql))
        affected_ids = [row[0] for row in rs]
        logger.info('Total number of tweets to reparse: %s', len(affected_ids))
        w_query = """\
        SELECT tw.json_data AS jd
        FROM UNNEST(:ids) AS t(raw_id)
        JOIN tweet AS tw ON tw.raw_id=t.raw_id
        """
        for chunk in chunked_iterable(affected_ids, bucket_size):
            for tn in delete_tables:
                del_tn = table_deletes_sql[tn]
                try:
                    session.execute(text(del_tn).bindparams(ids=chunk))
                    session.commit()
                    logger.info('Table %s deleted successfully!', tn)
                except SQLAlchemyError as err:
                    logger.exception(err)
                    raise
            rs = session.execute(text(w_query).bindparams(ids=chunk))
            jds = iter_rows_0(rs)
            parser.bulk_parse_and_save(
                jds,
                session,
                platform_id,
                multiprocesses=True,
                ignore_tables=ignore_tables)
            counter += len(chunk)
            logger.info('Current Number of reparsed tweets: %s', counter)
        logger.info('Total number of reparsed tweets: %s!', counter)
        logger.info('Reparse done, exit!')

    @classmethod
    def delete_tweet_table(cls, session, args):
        """Delete from tweet table where created_time is more than 6 months from the current data
        """
        six_months_ago = date.today() + relativedelta(months=-6)
        logger.warning(six_months_ago)

        table_deletes_sql ="""\
                DELETE FROM tweet 
                WHERE created_at < %s
                """
        conn = hoaxy_connection_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(table_deletes_sql, (six_months_ago,))
                conn.commit()
        except:
            conn.rollback()
            raise
        finally:
            conn.close()
            hoaxy_connection_pool.putconn(conn)
        logger.info('Tweets older than 6 months are being deleted !')

    @classmethod
    def run(cls, args):
        """Overriding method as the entry point of this command."""
        try:
            args = cls.args_schema.validate(args)
        except SchemaError as e:
            raise SystemExit('\n' + e + '\n')
        session = Session(expire_on_commit=False)
        if args['--twitter-streaming'] is True:
            configure_logging('twitter.streaming')
            cls.twitter_stream(session, args)
        elif args['--load-tweets'] is True:
            configure_logging('twitter.load-tweets')
            cls.load_tweets(session, args)
        elif args['--reparse-db-tweets'] is True:
            configure_logging('twitter.reparse-db', file_level='WARNING')
            cls._test_table_names(session, args)
            cls.reparse_db(session, args)
        elif args['--prune-db-tweets'] is True:
            configure_logging('twitter.prune-db-tweets', file_level='WARNING')
            cls.delete_tweet_table(session, args)
