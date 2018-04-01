from hoaxy.sns.twitter.parsers import BulkParser
from hoaxy.database import ENGINE
from hoaxy.database import Session
from hoaxy.database.models import TwitterUserUnion
from hoaxy.database.models import TwitterNetworkEdge
from sqlalchemy import text
import logging

from hoaxy import CONF
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker

from multiprocessing import Process, Manager
from os import sys
logger = logging.getLogger(__name__)


def init_tables(engine, drop_first=False):
    if drop_first is True:
        TwitterUserUnion.__table__.drop(engine, checkfirst=True)
        TwitterNetworkEdge.__table__.drop(engine, checkfirst=True)
    TwitterUserUnion.__table__.create(engine, checkfirst=True)
    TwitterNetworkEdge.__table__.create(engine, checkfirst=True)


def producer_queue(q, min_id=None, max_id=None, window_size=10000):
    engine = create_engine(
        URL(**CONF['database']['connect_args']),
        pool_size=1,
        pool_recycle=CONF['database']['pool_recycle'],
        client_encoding='utf8')
    if min_id is None:
        query = """ SELECT tw.id
                FROM tweet AS tw
                JOIN  twitter_network_edge AS te ON te.tweet_raw_id=tw.raw_id
                ORDER BY te.id DESC LIMIT 1"""
        min_id = engine.execute(text(query)).scalar()
        if min_id is None:
            min_id = 0
    if max_id is None:
        query = """ SELECT MAX(id) FROM tweet"""
        max_id = engine.execute(text(query)).scalar()
        if max_id is None:
            max_id = 0
            logger.error('No data in tweet table!')
            return None
    w_open_left = min_id
    w_close_right = min_id + window_size
    while True:
        if w_close_right > max_id:
            w_close_right = max_id
        if w_open_left >= max_id:
            logger.info(
                'All windows has been put into queue! Waiting for parsing and saving'
            )
            q.put('STOP')
            break
        q.put((w_open_left, w_close_right))
        w_open_left = w_close_right
        w_close_right += window_size


def consumer_queue(pid, q):
    engine = create_engine(
        URL(**CONF['database']['connect_args']),
        pool_size=1,
        pool_recycle=CONF['database']['pool_recycle'],
        client_encoding='utf8')
    Session = scoped_session(sessionmaker(bind=engine))
    session = Session()
    parser = BulkParser(
        platform_id=1,
        save_none_url_tweet=True,
        file_save_null_byte_tweet='null_byte_tweet.txt')

    while True:
        data = q.get()
        if data == 'STOP':
            logger.info('Subprocess %s: Task finished!', pid)
            q.put('STOP')
            break
        w_open_left, w_close_right = data
        jds = dict()
        g_urls_map = dict()
        query = """
            SELECT tw.id, tw.json_data, u.id, u.raw
            FROM tweet AS tw
            JOIN ass_tweet_url AS atu ON atu.tweet_id=tw.id
            JOIN url AS u ON u.id=atu.url_id
            WHERE tw.id>:l AND tw.id<=:r
            """
        for tw_id, jd, url_id, url in engine.execute(
                text(query).bindparams(l=w_open_left, r=w_close_right)):
            jds[tw_id] = jd
            if url_id is not None:
                g_urls_map[url] = url_id
        parser.bulk_parse_and_save(
            session=session,
            jds=jds,
            existed_tweets=True,
            g_urls_map=g_urls_map)


class ParserManager(object):

    def __init__(self, min_id, max_id, window_size, number_of_processes):
        self.manager = Manager()
        self.q = self.manager.Queue()
        self.min_id = min_id
        self.max_id = max_id
        self.window_size = window_size
        self.number_of_processes = number_of_processes

    def start(self):
        self.producer = Process(
            target=producer_queue,
            args=(self.q, self.min_id, self.max_id, self.window_size))
        self.producer.start()

        self.consumers = [
            Process(target=consumer_queue, args=(i, self.q))
            for i in range(self.number_of_processes)
        ]
        for consumer in self.consumers:
            consumer.start()

    def join(self):
        self.producer.join()
        for consumer in self.consumers:
            consumer.join()


def mp_main_test(min_id=None,
                 max_id=None,
                 window_size=10000,
                 number_of_processes=4,
                 drop_first=False):
    engine = create_engine(
        URL(**CONF['database']['connect_args']),
        pool_size=1,
        pool_recycle=CONF['database']['pool_recycle'],
        client_encoding='utf8')
    init_tables(engine, drop_first)
    try:
        manager = ParserManager(
            min_id=min_id,
            max_id=max_id,
            window_size=window_size,
            number_of_processes=number_of_processes)
        manager.start()
        manager.join()
    except (KeyboardInterrupt, SystemExit):
        logger.info('interrupt signal received')
        sys.exit(1)
    except Exception, e:
        raise e


if __name__ == '__main__':
    # setting sqlalchemy logging
    # logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    logger = logging.getLogger()
    logging.basicConfig(level='INFO')
    mp_main_test(
        min_id=None,
        max_id=None,
        window_size=1000,
        number_of_processes=8,
        drop_first=True)
