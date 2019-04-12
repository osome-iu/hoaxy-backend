from hoaxy.database import ENGINE as engine
from hoaxy.ir.search import db_query_network, db_query_network_old

import logging

logger = logging.getLogger(__name__)





gids = [1, 2, 3, 4, 5]

df1 = db_query_network(engine, ids=gids)
df2 = db_query_network_old(engine, ids=gids)

# number of retweet edges
unweighted_retweet_cols = ['tweet_id', 'from_user_id', 'to_user_id']
logger.info('New api, number of unweighted retweet edges is %s',
        len(df1[unweighted_retweet_cols].drop_duplicates()))
logger.info('Old api, number of unweighted retweet edges is %s',
        len(df2[unweighted_retweet_cols].drop_duplicates()))


cols = list(df1.columns.values)
cols.remove('url_id')

s1 = set(tuple(x) for x in df1[cols].values)
s2 = set(tuple(x) for x in df2[cols].values)
