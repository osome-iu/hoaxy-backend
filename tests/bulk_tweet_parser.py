from hoaxy.sns.twitter.parsers import BulkParser
from hoaxy.database import ENGINE
from hoaxy.database import Session
from hoaxy.database.models import TwitterUserUnion
from hoaxy.database.models import TwitterNetworkEdge
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)


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
    parser = BulkParser(
        platform_id=1,
        save_none_url_tweet=True,
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
    while True:
        jds = dict()
        g_urls_map = dict()
        q = """
            SELECT tw.id, tw.json_data, u.id, u.raw
            FROM tweet AS tw
            JOIN ass_tweet_url AS atu ON atu.tweet_id=tw.id
            JOIN url AS u ON u.id=atu.url_id
            WHERE tw.id>:l AND tw.id<=:r
            """
        if w_close_right > max_id:
            w_close_right = max_id
        if w_open_left >= max_id:
            logger.info('Max tweet id reached, Done!')
            break
        # q = """
        #     SELECT tw.id, tw.json_data, u.id, u.raw
        #     FROM tweet AS tw
        #     LEFT JOIN ass_tweet_url AS atu ON atu.tweet_id=tw.id
        #     LEFT JOIN url AS u ON u.id=atu.url_id
        #     WHERE tw.raw_id in (894686360900177920)
        #     """
        for tw_id, jd, url_id, url in engine.execute(
                text(q).bindparams(l=w_open_left, r=w_close_right)):
                # text(q)):
            jds[tw_id] = jd
            if url_id is not None:
                g_urls_map[url] = url_id
        w_open_left = w_close_right
        w_close_right += window_size
        # import pdb; pdb.set_trace()
        parser.bulk_parse_and_save(session=session, jds=jds,
                                   existed_tweets=True,
                                   g_urls_map=g_urls_map)
        # break

if __name__ == '__main__':
    # setting sqlalchemy logging
    # logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    logger = logging.getLogger()
    logging.basicConfig(level='INFO')
    session = Session()
    main_test(ENGINE, session, min_id=0, window_size=10000, drop_first=True)
    # main_test(ENGINE, session, window_size=1000, drop_first=False)
