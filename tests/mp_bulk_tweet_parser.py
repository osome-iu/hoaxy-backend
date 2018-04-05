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
from sqlalchemy.dialects.postgresql import insert

from multiprocessing import Process, Manager
from Queue import Empty
logger = logging.getLogger(__name__)


def init_tables(engine, drop_first=False):
    if drop_first is True:
        TwitterUserUnion.__table__.drop(engine, checkfirst=True)
        TwitterNetworkEdge.__table__.drop(engine, checkfirst=True)
    TwitterUserUnion.__table__.create(engine, checkfirst=True)
    TwitterNetworkEdge.__table__.create(engine, checkfirst=True)


def producer_queue(q1, min_id=None, max_id=None, window_size=10000):
    """Put window edges into q1."""
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
    logger.info('min_id=%s, max_id=%s, window_size=%s', min_id, max_id,
                window_size)
    w_open_left = min_id
    w_close_right = min_id + window_size
    while True:
        if w_close_right > max_id:
            w_close_right = max_id
        if w_open_left >= max_id:
            logger.info('Window edges have been put into q1.')
            q1.put('STOP')
            break
        q1.put((w_open_left, w_close_right))
        w_open_left = w_close_right
        w_close_right += window_size


def workers_queue(pid, q1, q2):
    """Receiving parameters from q1, then computing and finally put results
       into q2
    """
    engine = create_engine(
        URL(**CONF['database']['connect_args']),
        pool_size=1,
        pool_recycle=CONF['database']['pool_recycle'],
        client_encoding='utf8')
    Session = scoped_session(sessionmaker(bind=engine))
    session = Session()
    parser = BulkParser(platform_id=1, save_none_url_tweet=True)

    while True:
        try:
            data = q1.get(timeout=1)
        except Empty:
            logger.info('Worker process %s: queue is empty for 1 seconds', pid)
            q2.put((pid, 'STOP', None))
            break
        if data == 'STOP':
            logger.info('Worker process %s: STOP sign received from q1!', pid)
            q1.put('STOP')
            q2.put((pid, 'STOP', None))
            break
        else:
            logger.info('Worker process %s: data=%s received', pid, data)
        w_open_left, w_close_right = data
        jds = dict()
        g_urls_map = dict()
        query = """
            SELECT tw.id, tw.json_data, u.id, u.raw
            FROM tweet AS tw
            LEFT JOIN ass_tweet_url AS atu ON atu.tweet_id=tw.id
            LEFT JOIN url AS u ON u.id=atu.url_id
            WHERE tw.id>:l AND tw.id<=:r
            """
        for tw_id, jd, url_id, url in engine.execute(
                text(query).bindparams(l=w_open_left, r=w_close_right)):
            jds[tw_id] = jd
            if url_id is not None:
                g_urls_map[url] = url_id
        g_uusers_set = set()
        g_edges_set = set()
        for tw_id, jd in jds.iteritems():
            parser.parse_existed_one(
                tw_id,
                jd,
                session,
                g_urls_map=g_urls_map,
                g_uusers_set=g_uusers_set,
                g_edges_set=g_edges_set)
        edges = [
            dict(
                tweet_raw_id=t0,
                from_raw_id=t1,
                to_raw_id=t2,
                url_id=t3,
                is_quoted_url=t4,
                is_mention=t5,
                tweet_type=t6) for t0, t1, t2, t3, t4, t5, t6 in g_edges_set
            if t3 != -1
        ]
        uusers = [dict(raw_id=t1, screen_name=t2) for t1, t2 in g_uusers_set]
        # session.bulk_insert_mappings(TwitterNetworkEdge, edges)
        stmt_do_nothing = insert(TwitterNetworkEdge).values(
            edges).on_conflict_do_nothing(index_elements=[
                'tweet_raw_id', 'from_raw_id', 'to_raw_id', 'url_id',
                'is_quoted_url', 'is_mention', 'tweet_type'
            ])
        session.execute(stmt_do_nothing)
        session.commit()
        q2.put((pid, 'RUN', uusers))
        logger.info('Worker process %s: tweets from %s to %s done', pid,
                    w_open_left + 1, w_close_right)


def saver_queue(q2, number_of_workers):
    engine = create_engine(
        URL(**CONF['database']['connect_args']),
        pool_size=1,
        pool_recycle=CONF['database']['pool_recycle'],
        client_encoding='utf8')
    Session = scoped_session(sessionmaker(bind=engine))
    session = Session()
    workers_status = [1 for i in range(number_of_workers)]
    while True:
        pid, status, uusers = q2.get()
        if status == 'STOP':
            logger.info(
                'Saver process: STOP sign of worker process %s received from q2',
                pid)
            workers_status[pid] = 0
            if sum(workers_status) == 0:
                logger.warning('All STOP signs received from q2.')
                logger.warning('Data saving task done!')
                break
        else:
            logger.info('Saver process: size of uusers is %s', len(uusers))
            stmt_do_nothing = insert(TwitterUserUnion).values(
                uusers).on_conflict_do_nothing(index_elements=['raw_id'])
            session.execute(stmt_do_nothing)
            session.commit()


class ParserManager(object):

    def __init__(self, min_id, max_id, window_size, number_of_workers):
        self.manager = Manager()
        self.q1 = self.manager.Queue()
        self.q2 = self.manager.Queue()
        self.min_id = min_id
        self.max_id = max_id
        self.window_size = window_size
        self.number_of_workers = number_of_workers

    def start(self):
        self.producer = Process(
            target=producer_queue,
            args=(self.q1, self.min_id, self.max_id, self.window_size))
        self.producer.start()

        self.workers = [
            Process(target=workers_queue, args=(i, self.q1, self.q2))
            for i in range(self.number_of_workers)
        ]
        for workers in self.workers:
            workers.start()

        self.saver = Process(
            target=saver_queue, args=(self.q2, self.number_of_workers))
        self.saver.start()

    def join(self):
        self.producer.join()
        for workers in self.workers:
            workers.join()
        self.saver.join()


def mp_main_test(min_id=None,
                 max_id=None,
                 window_size=10000,
                 number_of_workers=4,
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
            number_of_workers=number_of_workers)
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
    logging.basicConfig(level='INFO', filename='mp_bulk_parser.log')
    # set up logging to console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # add the handler to the root logger
    logging.getLogger().addHandler(console)
    mp_main_test(
        min_id=100140535,
        max_id=None,
        window_size=10000,
        number_of_workers=10,
        drop_first=False)
