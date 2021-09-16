# -*- coding: utf-8 -*-
"""Hoaxy subcommand Report implementation.

This command provides method to report status and statistics about hoaxy
data collection.
"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

from botometer import NoTimelineError
from datetime import datetime
from datetime import timedelta
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from hoaxy.commands import HoaxyCommand
from hoaxy.database import ENGINE
from hoaxy.database import Session
from hoaxy.database.models import Top20ArticleMonthly
from hoaxy.database.models import Top20SpreaderMonthly
from hoaxy.ir.search import db_query_top_spreaders, db_query_top_articles
from hoaxy.utils.log import configure_logging
from requests import ConnectionError, Timeout, HTTPError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from tabulate import tabulate
from tweepy import TweepError
import botometer
import logging
import sys
import time

logger = logging.getLogger(__name__)


class Report(HoaxyCommand):
    """
Usage:
  hoaxy report --volume [--table=<table> --interval=<interval> --limit=<num>]
  hoaxy report --status=<twitter>
  hoaxy report --top-spreader --generate [--upper-day=<d>] [--force-today]
  hoaxy report --top-spreader --look-up  [--upper-day=<d>] [--most-recent]
  hoaxy report --top-article --generate [--upper-day=<d>] [--force-today]
  hoaxy report --top-article --look-up  [--upper-day=<d>] [--most-recent]

Report and generate statistics of hoaxy data collection.
(1) Aggregate statistics, e.g., every day, on data collection tables, including
    tweet, url and article
--volume                Report traffic volumn
--table=<table>         Specify table [default: tweet]
--interval=<interval>   Specify timeline interval [default: day], available
                          values are: minute, hour, day, week, month,
                            quarter and year.
--limit=<num>           Specify number of output items [default: 10]


(2) Report or generate the top spreaders and articles in the last 30 days.
--top-spreader          Report top spreaders
--top-article           Report top articles
--generate              Generate top spreader report.
--look-up               Look up top spreader history report.
--upper-day=<d>         The upper day of the top spreader calculation window.
--force-today           If set, force --upper-day=<utc-today>
--most-recent           If set, return most recent available data if there is
                        no data for <upper-day>

(3) Through the status of tweet table to test whether twitter streaming
is working.
--status=<twitter>      Report streaming status of <twitter>
    """
    name = 'report'
    short_description = 'Report and generate statistics of hoaxy collection'

    @classmethod
    def set_bot_or_not(cls, mspreaders):
        """Fetch bot score of top spreaders.

        Parameters
        ----------
            mspreaders : object
                An instance of model Top20SpreaderMonthly.
        """
        logger.info('Fetching bot score for top spreaders ...')
        rapid_key = cls.conf['botometer']['rapid_key']
        botometer_api_url = 'https://botometer-pro.p.rapidapi.com'
        logger.info(rapid_key)
        bon = botometer.Botometer(
            botometer_api_url=botometer_api_url,
            rapidapi_key=rapid_key,
            wait_on_ratelimit=True,
            **cls.conf['botometer']['twitter_app_credentials'])
        max_retries = 3
        num_retries = 0
        for ms in mspreaders:
            for num_retries in range(max_retries + 1):
                result = None
                try:
                    result = bon.check_account(ms.user_raw_id)
                except (TweepError, NoTimelineError) as e:
                    err_msg = '{}: {}'.format(
                        type(e).__name__,
                        getattr(e, 'msg', '') or getattr(e, 'reason', ''),
                    )
                    result = {'error': err_msg}
                except (Timeout, ConnectionError, HTTPError) as e:
                    if num_retries >= max_retries:
                        raise
                    else:
                        time.sleep(2**num_retries)
                if result is not None:
                    ms.bot_or_not = result
                    break

    @classmethod
    def print_psql(cls, df):
        """Print pandas.DataFrame using tabulate psql format."""
        s = tabulate(df, headers='keys', tablefmt='psql')
        logger.info('\n%s', s)

    @classmethod
    def look_up_top_spreaders(cls, session, upper_day, most_recent):
        """Look up top spreaders.

        Parameters
        ----------
        session : object
            An instance of SQLAlchemy Session.
        upper_day : date
            The right edge of 30 days window.
        most_recent : bool
            If no result in 30 days windows with `upper_day`, whether return
            most recently available data.
        """
        df = db_query_top_spreaders(session, upper_day, most_recent)
        if len(df) == 0:
            logger.warning('No result found!')
            cls.print_psql(df)
        else:
            logger.info('Top spreaders for day %s', upper_day)
            cls.print_psql(df)

    @classmethod
    def generate_top_spreaders(cls, session, upper_day):
        """Generate top spreaders.

        Parameters
        ----------
        session : object
            An instance of SQLAlchemy Session.
        upper_day : Date
            The right edge of 30 days window.
        """
        df = db_query_top_spreaders(session, upper_day)
        if len(df) > 0:
            logger.warning('Top spreaders for upper_day %s, already exist!',
                           upper_day)
            cls.print_psql(df)
            return
        month_delta = relativedelta(days=30)
        lower_day = upper_day - month_delta
        # top 20 most active spreaders for 'fact_checking'
        q1 = """
        INSERT INTO top20_spreader_monthly (upper_day, user_id, user_raw_id,
            user_screen_name, site_type, spreading_type, number_of_tweets)
        SELECT DISTINCT ON(tw.user_id)
            :upper_day AS upper_day,
            tw.user_id,
            tu.raw_id AS user_raw_id,
            atc.json_data#>>'{user, screen_name}' AS user_screen_name,
            'fact_checking' AS site_type,
            'active' AS spreading_type,
            t.number_of_tweets
        FROM
            (
                SELECT tw1.user_id, COUNT(tw1.id) AS number_of_tweets
                FROM tweet AS tw1
                    JOIN ass_tweet_url AS atu ON atu.tweet_id=tw1.id
                    JOIN url AS u ON u.id=atu.url_id
                    JOIN site AS s ON s.id=u.site_id
                WHERE tw1.created_at BETWEEN :lower_day AND :upper_day
                    AND site_type LIKE 'fact_checking'
                GROUP BY tw1.user_id
                ORDER BY number_of_tweets DESC LIMIT 20
            ) AS t
            JOIN tweet AS tw ON t.user_id=tw.user_id
            JOIN ass_tweet_content AS atc on atc.tweet_id=tw.id
            JOIN twitter_user AS tu ON tu.id=tw.user_id
        ORDER BY tw.user_id, tw.created_at DESC, t.number_of_tweets DESC
        """
        # top 20 most active spreaders for 'claim'
        q2 = """
        INSERT INTO top20_spreader_monthly (upper_day, user_id, user_raw_id,
            user_screen_name, site_type, spreading_type, number_of_tweets)
        SELECT DISTINCT ON(tw.user_id)
            :upper_day AS upper_day,
            tw.user_id,
            tu.raw_id AS user_raw_id,
            atc.json_data#>>'{user, screen_name}' AS user_screen_name,
            'claim' AS site_type,
            'active' AS spreading_type,
            t.number_of_tweets
        FROM
            (
                SELECT tw1.user_id, COUNT(tw1.id) AS number_of_tweets
                FROM tweet AS tw1
                    JOIN ass_tweet_url AS atu ON atu.tweet_id=tw1.id
                    JOIN url AS u ON u.id=atu.url_id
                    JOIN site AS s ON s.id=u.site_id
                WHERE tw1.created_at BETWEEN :lower_day AND :upper_day
                    AND site_type LIKE 'claim'
                GROUP BY tw1.user_id
                ORDER BY number_of_tweets DESC LIMIT 20
            ) AS t
            JOIN tweet AS tw ON t.user_id=tw.user_id
            JOIN ass_tweet_content AS atc on atc.tweet_id=tw.id
            JOIN twitter_user AS tu ON tu.id=tw.user_id
        ORDER BY tw.user_id, t.number_of_tweets DESC, tw.created_at DESC
        """
        # top 20 most influential spreaders for 'fact_checking'
        q3 = """
        INSERT INTO top20_spreader_monthly (upper_day, user_id, user_raw_id,
            user_screen_name, site_type, spreading_type, number_of_tweets)
        SELECT DISTINCT ON(tw.user_id)
            :upper_day AS upper_day,
            tw.user_id,
            tu.raw_id AS user_raw_id,
            atc.json_data#>>'{user, screen_name}' AS user_screen_name,
            'fact_checking' AS site_type,
            'influential' AS spreading_type,
            t.number_of_retweets
        FROM (
            SELECT COUNT(DISTINCT atw.id) AS number_of_retweets,
                    tu.id AS user_id
            FROM ass_tweet AS atw
                JOIN tweet AS tw ON tw.raw_id=atw.retweeted_status_id
                JOIN twitter_user AS tu ON tu.id=tw.user_id
                JOIN ass_tweet_url AS atu ON atu.tweet_id=tw.id
                JOIN url AS u ON u.id=atu.url_id
                JOIN site AS s ON s.id=u.site_id
            WHERE tw.created_at BETWEEN :lower_day AND :upper_day
                AND s.site_type LIKE 'fact_checking'
            GROUP BY tu.id
            ORDER BY number_of_retweets DESC LIMIT 20
            ) AS t
            JOIN tweet AS tw ON t.user_id=tw.user_id
            JOIN ass_tweet_content AS atc on atc.tweet_id=tw.id
            JOIN twitter_user AS tu ON tu.id=tw.user_id
        ORDER BY tw.user_id, tw.created_at DESC, t.number_of_retweets DESC
        """
        # top 20 most influential for 'claim'
        q4 = """
        INSERT INTO top20_spreader_monthly (upper_day, user_id, user_raw_id,
            user_screen_name, site_type, spreading_type, number_of_tweets)
        SELECT DISTINCT ON(tw.user_id)
            :upper_day AS upper_day,
            tw.user_id,
            tu.raw_id AS user_raw_id,
            atc.json_data#>>'{user, screen_name}' AS user_screen_name,
            'claim' AS site_type,
            'influential' AS spreading_type,
            t.number_of_retweets
        FROM (
            SELECT COUNT(DISTINCT atw.id) AS number_of_retweets,
                    tu.id AS user_id
            FROM ass_tweet AS atw
                JOIN tweet AS tw ON tw.raw_id=atw.retweeted_status_id
                JOIN twitter_user AS tu ON tu.id=tw.user_id
                JOIN ass_tweet_url AS atu ON atu.tweet_id=tw.id
                JOIN url AS u ON u.id=atu.url_id
                JOIN site AS s ON s.id=u.site_id
            WHERE tw.created_at BETWEEN :lower_day AND :upper_day
                AND s.site_type LIKE 'claim'
            GROUP BY tu.id
            ORDER BY number_of_retweets DESC LIMIT 20
            ) AS t
            JOIN tweet AS tw ON t.user_id=tw.user_id
            JOIN ass_tweet_content AS atc on atc.tweet_id=tw.id
            JOIN twitter_user AS tu ON tu.id=tw.user_id
        ORDER BY tw.user_id, tw.created_at DESC, t.number_of_retweets DESC
"""
        session.execute(
            text(q1).bindparams(lower_day=lower_day, upper_day=upper_day))
        session.execute(
            text(q2).bindparams(lower_day=lower_day, upper_day=upper_day))
        session.execute(
            text(q3).bindparams(lower_day=lower_day, upper_day=upper_day))
        session.execute(
            text(q4).bindparams(lower_day=lower_day, upper_day=upper_day))
        session.commit()
        mspeaders = session.query(Top20SpreaderMonthly).filter_by(
            upper_day=upper_day).all()
        if cls.conf['botometer']['enabled'] is True:
            cls.set_bot_or_not(mspeaders)
        try:
            session.commit()
            df = db_query_top_spreaders(session, upper_day)
            if len(df) == 0:
                logger.warning('No new top spreaders found!')
                cls.print_psql(df)
            else:
                logger.info('Insert new top spreaders for day %s', upper_day)
                cls.print_psql(df)
        except SQLAlchemyError as e:
            logger.error(e)
            session.rollback()

    @classmethod
    def look_up_top_articles(cls, session, upper_day, most_recent):
        """Look up top articles.

        Parameters
        ----------
        session : object
            An instance of SQLAlchemy Session.
        upper_day : date
            The right edge of 30 days window.
        most_recent : bool
            If no result in 30 days windows with `upper_day`, whether return
            most recently available data.
        """
        df = db_query_top_articles(session, upper_day, most_recent)
        if len(df) == 0:
            logger.warning('No result found!')
            cls.print_psql(df)
        else:
            logger.info('Top articles for day %s', upper_day)
            cls.print_psql(df)

    @classmethod
    def generate_top_articles(cls, session, upper_day):
        """Generate top articles.

        Parameters
        ----------
        session : object
            An instance of SQLAlchemy Session.
        upper_day : Date
            The right edge of 30 days window.
        """
        df = db_query_top_articles(
            session, upper_day, most_recent=False, exclude_tags=[])
        if len(df) > 0:
            logger.warning('Top spreaders for upper_day %s, already exist!',
                           upper_day)
            cls.print_psql(df)
            return
        month_delta = relativedelta(days=30)
        lower_day = upper_day - month_delta
        # top 20 most sharing articles for 'fact_checking'
        q1 = """
        INSERT INTO top20_article_monthly (upper_day, date_captured, title,
            canonical_url, site_type, number_of_tweets)
        SELECT :upper_day AS upper_day,
            a.date_captured AS date_captured,
            a.title AS title,
            a.canonical_url AS canonical_url,
            'fact_checking' AS site_type,
            COUNT(DISTINCT tw.id) AS number_of_tweets
        FROM article AS a
            JOIN url AS u ON u.article_id=a.id
            JOIN ass_tweet_url AS atu ON atu.url_id=u.id
            JOIN tweet AS tw ON tw.id=atu.tweet_id
            JOIN site AS s ON s.id=u.site_id
        WHERE a.date_captured BETWEEN :lower_day AND :upper_day
                AND tw.created_at BETWEEN :lower_day AND :upper_day
                AND s.site_type LIKE 'fact_checking'
        GROUP BY a.date_captured, a.title, a.canonical_url
        ORDER BY number_of_tweets DESC
        LIMIT 20
        """
        # top 20 most sharing articles for 'claim'
        q2 = """
        INSERT INTO top20_article_monthly (upper_day, date_captured, title,
            canonical_url, site_type, number_of_tweets)
        SELECT :upper_day AS upper_day,
            a.date_captured AS date_captured,
            a.title AS title,
            a.canonical_url AS canonical_url,
            'claim' AS site_type,
            COUNT(DISTINCT tw.id) AS number_of_tweets
        FROM article AS a
            JOIN url AS u ON u.article_id=a.id
            JOIN ass_tweet_url AS atu ON atu.url_id=u.id
            JOIN tweet AS tw ON tw.id=atu.tweet_id
            JOIN site AS s ON s.id=u.site_id
        WHERE a.date_captured BETWEEN :lower_day AND :upper_day
                AND tw.created_at BETWEEN :lower_day AND :upper_day
                AND s.site_type LIKE 'claim'
        GROUP BY a.date_captured, a.title, a.canonical_url
        ORDER BY number_of_tweets DESC
        LIMIT 20
        """
        session.execute(
            text(q1).bindparams(lower_day=lower_day, upper_day=upper_day))
        session.execute(
            text(q2).bindparams(lower_day=lower_day, upper_day=upper_day))
        try:
            session.commit()
            df = db_query_top_articles(session, upper_day)
            if len(df) == 0:
                logger.warning('No new top articles found!')
                cls.print_psql(df)
            else:
                logger.info('Insert new top articles for day %s', upper_day)
                cls.print_psql(df)
        except SQLAlchemyError as e:
            logger.error(e)
            session.rollback()

    @classmethod
    def run(cls, args):
        """Overriding method as the entry point of this command."""
        session = Session()
        if args['--volume'] is True:
            configure_logging(
                'report.volume',
                console_level=args['--console-log-level'],
                file_level='WARNING')
            table_names = ['tweet', 'url', 'article']
            table = args['--table']
            if table not in table_names:
                logger.critical('Available tables are: %s', table_names)
                sys.exit(2)
            interval_names = [
                'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year'
            ]
            interval = args['--interval']
            if interval not in interval_names:
                logger.critical('Available intervals are: %s', interval_names)
                sys.exit(2)
            limit = args['--limit']
            if int(limit) <= 0:
                logger.critical('%r should larger than 0', limit)
                sys.exit(2)
            sql = """
            SELECT count(id) as agg_num,
                date_trunc(:interval, created_at) as interval
            FROM %s
            GROUP BY interval
            ORDER BY interval DESC
            LIMIT :limit""" % table
            stmt = text(sql).bindparams(interval=interval, limit=limit)
            strf = '%Y-%m-%d %H:%M:%S'
            with ENGINE.connect() as conn:
                result = conn.execute(stmt).fetchall()
                print(('-' * 35))
                print(('{0:^20s} | {1:12s}'.format('Timeline (%s)' % interval,
                                                  'Aggregation')))
                print(('-' * 35))
                for v, t in result:
                    print(('{0:^20s} | {1:8d}'.format(t.strftime(strf), v)))
                print(('-' * 35))
        elif args['--status']:
            configure_logging(
                'report.streaming-status',
                console_level=args['--console-log-level'])
            table_name = None
            if args['--status'] == 'twitter':
                table_name = 'tweet'
            if table_name is None:
                logger.critical('SNS %r has not been implemented!',
                                args['--status'])
                sys.exit(2)
            sql = 'SELECT created_at FROM {} ORDER BY id DESC LIMIT 1'.format(
                'tweet')
            with ENGINE.connect() as conn:
                most_recent, = conn.execute(text(sql)).fetchone()
                delta_minutes = 30
                delta = timedelta(minutes=delta_minutes)
                current_utc = datetime.utcnow()
                if current_utc - most_recent > delta:
                    logger.critical(
                        'No %s streaming update in the past %s minutes!',
                        args['--status'], delta_minutes)
                else:
                    logger.info('Most recent %s streaming update is %s',
                                args['--status'],
                                str(most_recent) + ' (UTC)')
        elif args['--top-spreader'] is True:
            configure_logging(
                'report.top-spreaders',
                console_level=args['--console-log-level'],
                file_level='WARNING')
            # try to create table
            if (Top20SpreaderMonthly.__table__.exists(bind=ENGINE)) is False:
                Top20SpreaderMonthly.__table__.create(bind=ENGINE)
            if args['--force-today'] is True:
                upper_day = datetime.utcnow().date()
            elif args['--upper-day'] is None:
                upper_day = datetime.utcnow().date() - timedelta(days=1)
            else:
                try:
                    upper_day = parse(args['--upper-day']).date()
                except Exception:
                    raise ValueError('Invalid date: %s', args['--upper-day'])
            if args['--generate'] is True:
                logger.warning(
                    'Generating top spreaders for uppder_day=%r ...',
                    upper_day)
                cls.generate_top_spreaders(session, upper_day)
            elif args['--look-up'] is True:
                cls.look_up_top_spreaders(session, upper_day,
                                          args['--most-recent'])
        elif args['--top-article'] is True:
            configure_logging(
                'report.top-article',
                console_level=args['--console-log-level'],
                file_level='WARNING')
            # try to create table
            if (Top20ArticleMonthly.__table__.exists(bind=ENGINE)) is False:
                Top20ArticleMonthly.__table__.create(bind=ENGINE)
            if args['--force-today'] is True:
                upper_day = datetime.utcnow().date()
            elif args['--upper-day'] is None:
                upper_day = datetime.utcnow().date() - timedelta(days=1)
            else:
                try:
                    upper_day = parse(args['--upper-day']).date()
                except Exception:
                    raise ValueError('Invalid date: %s', args['--upper-day'])
            if args['--generate'] is True:
                logger.warning('Generating top articles for uppder_day=%r ...',
                               upper_day)
                cls.generate_top_articles(session, upper_day)
            elif args['--look-up'] is True:
                cls.look_up_top_articles(session, upper_day,
                                         args['--most-recent'])
        session.close()
