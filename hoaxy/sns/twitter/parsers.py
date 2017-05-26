# -*- coding: utf-8 -*-
"""Implement parser to parse tweet received from twitter streaming.
"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

from hoaxy.database import Session
from hoaxy.database.functions import get_or_create_m
from hoaxy.database.functions import get_or_create_murl
from hoaxy.database.models import AssTweetHashtag
from hoaxy.database.models import AssTweetUrl
from hoaxy.database.models import Hashtag
from hoaxy.database.models import Tweet
from hoaxy.database.models import TwitterUser
from hoaxy.utils.dt import utc_from_str
from sqlalchemy import text
from sqlalchemy.exc import DataError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import SQLAlchemyError
import Queue
import logging
import threading
import time

logger = logging.getLogger(__name__)


class Parser():
    """This class parses a tweet and save it into database.

    This class examines every URL in the `entities` of all tweet object
    in the tweet, including quoted tweet and retweet. Then match the URL
    with exsited site domains and save them into database. At same time,
    the tweet itself, the user of this tweet, and all hashtags will be saved
    into database.
    """

    def __init__(self, session, platform_id, save_none_url_tweet=True):
        """Constructor of Parser.

        Parameters
        ----------
        session : object
            An instance of SQLAlchemy Session.
        platform_id : int
            The id of platform (should be twitter platform object).
        save_none_url_tweet : bool
            Whether save the tweet if no URLs in the tweet.
        """
        self.session = session
        self.save_none_url_tweet = save_none_url_tweet
        self.platform_id = platform_id

    def _parse_entities(self, entities, urls_set, hashtags_set):
        """Internal function to parse entities in a tweet object.

        By default postgresql column is case-sensitive and URL itself may
        be case sensitive, no need to convert case. However, for hashtags,
        twitter use them as case-insensitive, so we need to convert them
        into lower case.
        """
        if 'urls' in entities:
            for u in entities['urls']:
                try:
                    url = u['expanded_url']
                    if url:
                        urls_set.add(url)
                except Exception as e:
                    logger.error(e)
                    continue
        # hashtag in twitter is case insensitive, shall convert
        if 'hashtags' in entities:
            for h in entities['hashtags']:
                if h.get('text'):
                    hashtags_set.add(h['text'].lower())

    def parse(self, jd):
        """The main parse function.

        Parameters
        ---------
        jd : json
            Tweet json data.

        Procedures
        ----------
        1) validate `jd`
        2) extract URL and hashtag from `jd`
        3) insert into database
        """
        logger.debug('Parsing one tweet, begin')
        #
        # validation
        #
        try:
            tw_raw_id = jd['id']
            created_at = utc_from_str(jd['created_at'])
            user_raw_id = jd['user']['id']
        except KeyError as e:
            logger.error('Invalid tweet: %s', e)
            return None
        #
        # extract url, hashtag and associated tweet status id
        #
        urls_set = set()
        hashtags_set = set()
        entities_list = []
        if 'entities' in jd:
            entities_list.append(jd['entities'])
        if 'quoted_status' in jd:
            q_jd = jd['quoted_status']
            if 'entities' in q_jd:
                entities_list.append(q_jd['entities'])
        if 'retweeted_status' in jd:
            re_jd = jd['retweeted_status']
            if 'entities' in re_jd:
                entities_list.append(re_jd['entities'])
            if 'quoted_status' in re_jd and\
                    'entities' in re_jd['quoted_status']:
                entities_list.append(re_jd['quoted_status']['entities'])
        for entities in entities_list:
            if entities:
                self._parse_entities(entities, urls_set, hashtags_set)
        # This tweet should contain urls
        if len(urls_set) == 0 and self.save_none_url_tweet is False:
            logger.debug('No url found in %s, ignore!', tw_raw_id)
            return None
        #
        # Insert into database
        #
        # creating user
        logger.debug('creating user')
        muser = get_or_create_m(self.session, TwitterUser,
                                data=dict(raw_id=user_raw_id), fb_uk='raw_id')
        # creating tweet
        logger.debug('creating tweet')
        mtweet = Tweet(raw_id=tw_raw_id, json_data=jd,
                       created_at=created_at, user_id=muser.id)
        self.session.add(mtweet)
        try:
            self.session.commit()
            logger.debug('Inserted tweet %r', tw_raw_id)
        except IntegrityError as e:
            logger.warning('Tweet %s existed in db: %s', tw_raw_id, e)
            self.session.rollback()
            return
        # creating urls
        logger.debug('creating urls')
        for url in urls_set:
            murl = get_or_create_murl(
                self.session,
                data=dict(raw=url),
                platform_id=self.platform_id)
            self.session.add(AssTweetUrl(tweet_id=mtweet.id, url_id=murl.id))
            try:
                self.session.commit()
            except IntegrityError as e:
                logger.error('ass_tweet_url IntegrityError, see: %s', e)
                self.session.rollback()
        # creating hashtags
        logger.debug('creating hashtags')
        for hashtag in hashtags_set:
            mhashtag = get_or_create_m(self.session, Hashtag,
                                       data=dict(text=hashtag), fb_uk='text')
            self.session.add(AssTweetHashtag(tweet_id=mtweet.id,
                                             hashtag_id=mhashtag.id))
            try:
                self.session.commit()
            except IntegrityError as e:
                logger.error('ass_tweet_hashtag IntegrityError, see: %s', e)
                self.session.rollback()
        # paring associate tweet
        q1 = """
INSERT INTO ass_tweet (id, retweeted_status_id, quoted_status_id,
                      in_reply_to_status_id)
    SELECT id,
        CAST(json_data#>>'{retweeted_status, id}' AS BIGINT),
        CAST(json_data#>>'{quoted_status, id}' AS BIGINT),
        CAST(json_data->>'in_reply_to_status_id' AS BIGINT)
    FROM tweet
    WHERE id=:tweet_id
"""
        q1 = text(q1).bindparams(tweet_id=mtweet.id)
        try:
            self.session.execute(q1)
            self.session.commit()
        except DataError as e:
            # Handle \u0000 exception that postgresql json do not support
            logger.warning(e)
            self.session.rollback()
            q2 = r"""
UPDATE tweet SET json_data=regexp_replace(
            json_data::text, '\\u0000', '\\\\u0000', 'g')::json
WHERE id=:tweet_id
    """
            q2 = text(q2).bindparams(tweet_id=mtweet.id)
            self.session.execute(q2)
            self.session.commit()
            logger.warning('json_data is updated (\\u0000 to \\\\u0000)')
            self.session.execute(q1)
            self.session.commit()
        logger.debug('Parsing one tweet, done.')


class QueueParser(object):
    """This class implement threaded parser by queue.

    This class first put tweet into the queue, then another thread consume the
    tweet in the queue.

    This class also provides a way to resist temporal failure of database
    connection.
    """
    # The sentinel to stop thread.
    _sentinel = None

    def __init__(self, queue, platform_id, window_size=1000, **p_kwargs):
        """Constructor of QueueParser.

        Parameters
        ----------
        queue : queue
            A queue instance.
        platform_id : int
            The id of a platform record.
        window_size : int
            The windows size to generate logging information.
        """
        self.queue = queue
        self.platform_id = platform_id
        self.p_kwargs = p_kwargs
        self._stop = threading.Event()
        self._thread = None
        self._counter = 0
        self._window_size = window_size
        # When PostgreSQL is down,
        # set hold_on = True, which will keep data for a maximum of 3 days
        # During hold-on, periodically try to reconnect server.
        self._hold_on = False
        self._hold_on_counter = 0
        self._hold_on_unit = 30
        self._hold_on_max = 3 * 24 * 3600

    def dequeue(self, block):
        """Dequeue a tweet and return it, optionally blocking."""
        return self.queue.get(block)

    def start(self):
        """Start the parser.

        This starts up a background thread to monitor the queue for
        tweet to process.
        """
        self._thread = t = threading.Thread(target=self._monitor)
        t.setDaemon(True)
        t.start()

    def stop (self):
        """Stop the listener.

        This asks the thread to terminate, and then waits for it to do so.
        Note that if you don't call this before your application exits, there
        may be some records still left on the queue, which won't be processed.
        """
        self._stop.set()
        self.enqueue_sentinel()
        self._thread.join()
        self._thread = None

    def restart(self):
        """Restart the listener."""
        if self._thread is not None:
            self.start()
        else:
            self.stop()
            self.start()

    def is_alive(self):
        """Test whether the consuming thread is alive"""
        return self._thread.is_alive()

    def _test_connection(self, session):
        """Test whether the database connection resumed."""
        try:
            session.execute('select 1').fetchall()
            return True
        except SQLAlchemyError as e:
            logger.info(e)
            session.rollback()
            return False
        except Exception as e:
            logger.error(e)
            return False

    def _monitor(self):
        """Monitor the queue for tweet, and use function parse to parse it.

        This method runs on a separate, internal thread.
        The thread will terminate if it sees a sentinel object in the queue.
        """
        # scoped_session
        # Session itself is not thread safe, use scoped_session
        # each thread use only one scoped_session object
        # We never delete anything from database, and we rely much
        # on the `id` of  existed object to build relaship
        # set expire_on_commit=False
        # to avoid re-fetch of these existed objects
        session = Session(expire_on_commit=False)
        parser = Parser(session, self.platform_id, **self.p_kwargs)
        q = self.queue
        has_task_done = hasattr(q, 'task_done')
        while not self._stop.isSet():
            # Server down, hold on
            if self._hold_on is True:
                logger.info('qsize is %s', q.qsize())
                time.sleep(self._hold_on_unit)
                self._hold_on_counter += self._hold_on_unit
                if self._hold_on_counter >= self._hold_on_max:
                    return
                logger.info('Hold on, keep tring to connect SQL server...')
                logger.info('Elapsed %s seconds, since recent server down',
                            self._hold_on_counter)
                if self._test_connection(session):
                    self._hold_on = False
                    self._hold_on_counter = 0
                continue
            try:
                jd = self.dequeue(True)
                if jd is self._sentinel:
                    break
                self._counter += 1
                if self._counter % self._window_size == 0:
                    logger.info('qsize is %s', q.qsize())
                parser.parse(jd)
                if has_task_done:
                    q.task_done()
            except Queue.Empty:
                break
            except Exception as e:
                logger.error('Exception %s when parsing %s', e, jd)
                if isinstance(e, SQLAlchemyError):
                    session.rollback()
                    if isinstance(e, OperationalError):
                        # if 'could not connect to server' in str(e):
                        logger.error('Hold on until SQL service back! %s', e)
                        self._hold_on = True
        # There might still be records in the queue.
        while True:
            try:
                jd = self.dequeue(False)
                if jd is self._sentinel:
                    break
                parser.parse(jd)
                if has_task_done:
                    q.task_done()
            except Queue.Empty:
                break
            except Exception as e:
                logger.error('Exception %s when parsing %s', e, jd)
                if isinstance(e, SQLAlchemyError):
                    session.rollback()
                    if isinstance(e, OperationalError):
                        return

    def enqueue_sentinel(self):
        """This is used to enqueue the sentinel record.

        The base implementation uses put_nowait. You may want to override this
        method if you want to use timeouts or work with custom queue
        implementations.
        """
        self.queue.put_nowait(self._sentinel)
