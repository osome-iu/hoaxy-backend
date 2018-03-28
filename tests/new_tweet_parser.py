from hoaxy.sns.twitter.parsers import Parser
# from hoaxy.database import ENGINE
# from hoaxy.database import Session
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

# if you want to logging the actual queries, use the following setting.
# import logging
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# try:
#     ENGINE = create_engine(
#         URL(**CONF['database']['connect_args']),
#         pool_size=CONF['database']['pool_size'],
#         pool_recycle=CONF['database']['pool_recycle'],
#         client_encoding='utf8')
#     Session = scoped_session(sessionmaker(bind=ENGINE))
# except Exception:
#     raise
# logger = logging.getLogger(__name__)


def init_tables(engine, drop_first=False):
    if drop_first is True:
        TwitterUserUnion.__table__.drop(engine, checkfirst=True)
        TwitterNetworkEdge.__table__.drop(engine, checkfirst=True)
    TwitterUserUnion.__table__.create(engine, checkfirst=True)
    TwitterNetworkEdge.__table__.create(engine, checkfirst=True)


def main_test(engine,
              session,
              min_id=None,
              max_id=None,
              window_size=1000,
              drop_first=False):
    parser = Parser(
        session,
        platform_id=1,
        saved_tweet=True,
        file_save_null_byte_tweet='null_byte_tweet.txt')
    # init tables
    init_tables(engine, drop_first)
    if min_id is None:
        q = """ SELECT tw.id
                FROM tweet AS tw
                JOIN  twitter_network_edge AS te ON te.tweet_raw_id=tw.raw_id
                ORDER BY te.id DESC LIMIT 1"""
        min_id = engine.execute(text(q)).scalar()
        if min_id is None:
            min_id = 0
    if max_id is None:
        q = """ SELECT MAX(id) FROM tweet"""
        max_id = engine.execute(text(q)).scalar()
        if max_id is None:
            max_id = 0
            logger.error('No data in tweet table!')
            return None
    w_open_left = min_id
    w_close_right = min_id + window_size
    counter = min_id
    while True:
        logger.info('Current paring tweet id is %s ...', counter)
        q = """
            SELECT tw.json_data
            FROM tweet AS tw
            WHERE tw.id>:l AND tw.id<=:r
            ORDER BY tw.id
            """
        if w_close_right > max_id:
            w_close_right = max_id
        if w_open_left >= max_id:
            logger.info('Max tweet id reached, Done!')
            break
        for jd, in engine.execute(
                text(q).bindparams(l=w_open_left, r=w_close_right)):
            try:
                parser.parse(jd)
            except Exception:
                logger.error('Tweet raw id is: %s', jd['id'])
                raise
            counter += 1
        w_open_left = w_close_right
        w_close_right += window_size
    if parser.fp:
        parser.fp.close()


def producer_queue(q, min_id=None, max_id=None, window_size=5000):
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
        cqsize = q.qsize()
        if cqsize < window_size:
            logger.info('Current qsize (should be less than window_size): %s',
                        cqsize)
            logger.info('Put block %s to %s into queue...', w_open_left + 1,
                        w_close_right)
            query = """
                SELECT tw.json_data
                FROM tweet AS tw
                WHERE tw.id>:l AND tw.id<=:r
                ORDER BY tw.id
                """
            if w_close_right > max_id:
                w_close_right = max_id
            if w_open_left >= max_id:
                logger.info('Max tweet id reached, Done!')
                q.put('STOP')
                break
            for jd, in engine.execute(
                    text(query).bindparams(l=w_open_left, r=w_close_right)):
                q.put(jd)
            w_open_left = w_close_right
            w_close_right += window_size


def consumer_queue(q):
    engine = create_engine(
        URL(**CONF['database']['connect_args']),
        pool_size=1,
        pool_recycle=CONF['database']['pool_recycle'],
        client_encoding='utf8')
    Session = scoped_session(sessionmaker(bind=engine))
    session = Session()
    parser = Parser(
        session,
        platform_id=1,
        saved_tweet=True,
        file_save_null_byte_tweet='null_byte_tweet.txt')
    e_fp = open('exception_tweet.txt', 'w')
    while True:
        jd = q.get()
        if jd == 'STOP':
            logger.info('Task finished!')
            q.put('STOP')
            break
        try:
            parser.parse(jd)
        except Exception as e:
            logger.warning('Exception: %s, when parsing: %s', e, jd['id'])
            e_fp.write(jd['id'])
            e_fp.write('\n')
    e_fp.close()


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
            Process(target=consumer_queue, args=(self.q,))
            for i in range(self.number_of_processes)
        ]
        for consumer in self.consumers:
            consumer.start()

    def join(self):
        self.producer.join()
        for consumer in self.consumers:
            consumer.join()


def main_mulprocessing(min_id=None,
                       max_id=None,
                       window_size=5000,
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
    # session = Session()
    # import pdb; pdb.set_trace()
    # main_test(ENGINE, session, min_id=0, window_size=1000, drop_first=True)
    # main_test(ENGINE, session, window_size=1000, drop_first=False)
    main_mulprocessing(
        min_id=None,
        max_id=None,
        window_size=5000,
        number_of_processes=10,
        drop_first=False)
