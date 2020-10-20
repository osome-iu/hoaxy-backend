from hoaxy.database import ENGINE as engine
from hoaxy.ir.search import db_query_network
from hoaxy.ir.search import db_query_filter_disabled_site
from hoaxy.ir.search import db_query_twitter_shares
from hoaxy.backend.api import searcher
from hoaxy.backend.api import N1, N2, MIN_SCORE, STRAMING_START_AT
from sqlalchemy import text
import numpy as np
import pandas as pd
import logging
import time

logger = logging.getLogger(__name__)


def wrapper(func, *args, **kwargs):

    def wrapped():
        return func(*args, **kwargs)

    return wrapped


def chunk_seq(seq, n_chunk):
    out = []
    n = len(seq)
    chunk_size = n // n_chunk
    reminder = n % n_chunk
    first_right_edge = chunk_size + reminder
    out.append(seq[:first_right_edge])
    s = first_right_edge
    e = s + chunk_size
    while True:
        if s >= n:
            break
        out.append(seq[s:e])
        s = e
        e += chunk_size
    return out


def prepare_queries(engine, w_per_q=5):
    q = """ SELECT title
            FROM article
            ORDER BY RANDOM()
            LIMIT 1000
        """
    rs = engine.execute(text(q))
    words = []
    for title, in rs:
        words += title.split(' ')
    np.random.shuffle(words)
    return [
        ' '.join(words[x:x + w_per_q]) for x in range(0, len(words), w_per_q)
    ]


def prepare_network_queries(engine, ids_per_q=10):
    q = """ SELECT group_id
            FROM article
            ORDER BY RANDOM
            LIMIT 10000
        """
    ids = [r for r, _ in rs]
    return chunk_seq(ids, ids_per_q)


# all in one test
def test_api_flow(n1=N1,
                  n2=N2,
                  min_score=MIN_SCORE,
                  min_date_published=STRAMING_START_AT,
                  query='',
                  sort_by='relevant',
                  use_lucene_syntax=False):
    try:
        t0 = time.time()
        logger.debug('Starting lucene query %r at %r', query, t0)
        n, df = searcher.search(
            n1=N1,
            n2=N2,
            min_score_of_recent_sorting=MIN_SCORE,
            min_date_published=STRAMING_START_AT,
            query=query,
            sort_by=sort_by,
            use_lucene_syntax=use_lucene_syntax)
        t1 = time.time()
        logger.debug('Starting filtering disabled sites at %r', t1)
        df = db_query_filter_disabled_site(engine, df)
        t2 = time.time()
        logger.debug('Starting querying number tweets sharing at %r', t2)
        df = db_query_twitter_shares(engine, df)
        if len(df) == 0:
            raise APINoResultError('No article found!')
        # sort dataframe by 'number_of_tweets'
        df = df.sort_values('number_of_tweets', ascending=False)
        t3 = time.time()
        ids = df.iloc[:10].id.tolist()
        logger.debug('Starting querying network %s by old api at %r', ids, t3)
        # df = db_query_network_old(engine, ids=ids)
        # t4 = time.time()
        # logger.debug('Starting querying network %s by new api at %r', ids, t4)
        df = db_query_network(engine, ids=ids)
        t5 = time.time()
    except Exception as e:
        logger.error(e)
        return None
    return dict(
        t0_lucene_query=(t1 - t0),
        t1_article_filtering=(t2 - t1),
        t2_article_sharing=(t3 - t2),
        t3_network_building_old=(t4 - t3),
        t4_network_buiding_new=(t5 - t4))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    queries = prepare_queries(engine)
    results = []
    for query in queries[:100]:
        r = test_api_flow(query=query)
        if r is not None:
            results.append(r)
    df = pd.DataFrame(results)
    df.to_csv('performance.test.csv')
    logger.info(df.mean(axis=0))
