# -*- coding: utf-8 -*-
"""Lucene search module.

This module provide a Searcher class and other functions for search function.
The backend APIs mainly rely on this module.
"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

from datetime import datetime
from datetime import timedelta
from os.path import isfile
import logging
import re

from hoaxy.database import Session
from hoaxy.database.functions import get_max
from hoaxy.database.models import Top20ArticleMonthly
from hoaxy.database.models import Top20SpreaderMonthly
from hoaxy.exceptions import APINoResultError
from hoaxy.exceptions import APIParseError
from hoaxy.utils.dt import utc_from_str
from java.io import File
from java.lang import Float
from java.util import HashMap
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.queries import ChainedFilter
from org.apache.lucene.queryparser.classic import MultiFieldQueryParser
from org.apache.lucene.sandbox.queries import DuplicateFilter
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.search import Sort
from org.apache.lucene.search import SortField
from org.apache.lucene.search import TermRangeFilter
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.util import BytesRef
from sqlalchemy import text
import networkx as nx
import pandas as pd

logger = logging.getLogger(__name__)
LUCENE_RESERVED_CH_RE = re.compile(r'[\+\-!\(\)\{\}\[\]\^\"~\*\?:\\/]')


def clean_query(q):
    """Remove lucene reserved characters."""
    q = q.replace('&&', ' ')
    q = q.replace('||', ' ')
    q = re.sub(LUCENE_RESERVED_CH_RE, ' ', q)
    return q


class Searcher():
    """A simple interface to search articles.

    In this class `MultiFieldQueryParse`, `DuplicateFilter` are used to
    accomplish our application: query should apply on multiple fields,
    duplication should be avoid.
    """

    def __init__(self,
                 index_dir,
                 search_fields=['canonical_url', 'title', 'meta', 'content'],
                 unique_field='uq_id_str',
                 boost=dict(
                     canonical_url=4.0, title=8.0, meta=2.0, content=1.0),
                 date_format='%Y-%m-%dT%H:%M:%S'):
        """Constructor of Searcher.

        Parameters
        ----------
        index_dir : string
            The location of lucene index.
        search_fields : list
            A list of field names indicating fields to search on.
        unique_field : string
            The field name, on which the duplication should avoid.
        boost : dict
            This dict control the weight when computing score.
        date_format : string
            Convert the string into datetime. Should consistent with the
            index part.
        """
        self.index_dir = index_dir
        self.search_fields = search_fields
        self.sort_by_recent = Sort(
            SortField('date_published', SortField.Type.STRING, True))
        self.store = FSDirectory.open(File(index_dir))
        self.reader = DirectoryReader.open(self.store)
        self.isearcher = IndexSearcher(self.reader)
        self.analyzer = StandardAnalyzer()
        self.dup_filter = DuplicateFilter(unique_field)
        self.boost_map = HashMap()
        for k, v in boost.iteritems():
            self.boost_map.put(k, Float(v))
        self.mul_parser = MultiFieldQueryParser(search_fields, self.analyzer,
                                                self.boost_map)
        self.date_format = date_format

    def prepare_chained_filter(self, dt1, dt2):
        """Return a chained filter."""
        return ChainedFilter([
            self.dup_filter,
            TermRangeFilter('date_published',
                            BytesRef(dt1.strftime(self.date_format)),
                            BytesRef(dt2.strftime(self.date_format)), True,
                            True)
        ], [ChainedFilter.AND, ChainedFilter.AND])

    def refresh(self):
        """Refresh the searsher, if index is changed."""
        nireader = DirectoryReader.openIfChanged(self.reader)
        if nireader:
            self.reader.close()
            self.reader = nireader
            self.isearcher = IndexSearcher(self.reader)
            logger.debug('Index file changed, freshed')
        else:
            logger.debug('Index file did not change.')

    def fetch_one_doc(self, score_doc):
        """Fetch one document from the scored doc results."""
        doc = self.isearcher.doc(score_doc.doc)
        return (
            doc.getField("group_id").numericValue().intValue(),
            doc.get("canonical_url"),
            doc.get("title"),
            doc.get("date_published"),
            doc.get("domain"),
            doc.get("site_type"),
            score_doc.score,)

    def search(self,
               query,
               n1=100,
               n2=100000,
               sort_by='relevant',
               use_lucene_syntax=False,
               min_score_of_recent_sorting=0.4,
               min_date_published=None):
        """Return the matched articles from lucene.

        Parameters
        ----------
        query : string
            The query string.
        n1 : int
            How many result finally returned.
        n2 : int
            How many search results returned when sort by recent.
        sort_by : string
            {'relevant', 'recent'}, the sorting order when doing lucene searching.
        min_score_of_recent_sorting : float
            The min score when sorting by 'recent'.
        min_date_published : datetime<Plug>(neosnippet_expand)
            The min date_published when filtering lucene searching results.

        Returns
        -------
        tuple
            (total_hits, df), where total_hits represents the total number
            of hits and df is a pandas.DataFrame object. df.columns = ['id',
            'canonical_url', 'title', 'date_published', 'domain', 'site_type',
            'score']
        """
        if min_date_published is not None:
            dt2 = datetime.utcnow()
            if isinstance(min_date_published, datetime):
                dt1 = min_date_published
            elif isinstance(min_date_published, basestring):
                dt1 = utc_from_str(min_date_published)
            sf = self.prepare_chained_filter(dt1, dt2)
        else:
            sf = self.dup_filter
        try:
            if use_lucene_syntax is False:
                query = clean_query(query)
            q = self.mul_parser.parse(self.mul_parser, query)
            logger.debug('Parsed query: %s', q)
        except Exception as e:
            logger.error(e)
            if use_lucene_syntax is True:
                raise APIParseError("""Error when parse the query string! \
You are quering with lucene syntax, be careful of your query string!""")
            else:
                raise APIParseError('Error when parse the query string!')

        cnames = [
            'id', 'canonical_url', 'title', 'date_published', 'domain',
            'site_type', 'score'
        ]
        if sort_by == 'relevant':
            top_docs = self.isearcher.search(q, sf, n1)
            score_docs = top_docs.scoreDocs
            total_hits = top_docs.totalHits
            if total_hits == 0:
                df = pd.DataFrame()
            else:
                records = [self.fetch_one_doc(sd) for sd in score_docs]
                df = pd.DataFrame(records, columns=cnames)
                df['date_published'] = pd.to_datetime(df['date_published'])
            return total_hits, df
        elif sort_by == 'recent':
            counter = 0
            records = []
            top_field_docs = self.isearcher.search(
                q, sf, n2, self.sort_by_recent, True, True)
            if top_field_docs.maxScore >= min_score_of_recent_sorting:
                for sd in top_field_docs.scoreDocs:
                    if sd.score >= min_score_of_recent_sorting:
                        records.append(self.fetch_one_doc(sd))
                        counter += 1
                        if counter == n1:
                            break
            if counter == 0:
                df = pd.DataFrame()
            else:
                df = pd.DataFrame(records, columns=cnames)
                df['date_published'] = pd.to_datetime(df['date_published'])
            return counter, df


def db_query_filter_disabled_site(engine, df):
    """Filter out sites that has been disabled.

    After disabling a site, articles from this site are still store in the
    Lucene indexes. By query the database, we could filter out them.

    Parameters
    ----------
    engine : object
        A SQLAlchemy connection, e.g., engine or session.
    df : pd.DataFrame
        A DataFrame that represents article information, specifictly returned
        from Lucene search.

    Returns
    -------
    pd.DataFrame. Articles from disabled site are removed.
    """
    if len(df) == 0:
        return df
    q = """
    SELECT DISTINCT s.domain AS domain
    FROM UNNEST(:domains) AS t(domain)
        JOIN site AS s ON s.domain=t.domain
    WHERE s.is_enabled IS TRUE
    """
    rs = engine.execute(text(q).bindparams(domains=list(df.domain.unique())))
    return df.loc[df.domain.isin([r[0] for r in rs])]


def db_query_filter_tags(engine, df, exclude_tags):
    """Filter out sites that with specified tags.

    Parameters
    ----------
    engine : object
        A SQLAlchemy connection, e.g., engine or session.
    df : pd.DataFrame
        A DataFrame that represents article information, specifically returned
        from Lucene search.
    exclude_tags: list
        A list of excluded tags. The tag could be the name of string or tuple
        (source, name)

    Returns
    -------
    pd.DataFrame. Articles from disabled site are removed.
    """
    if len(df) == 0:
        return df
    tag_df = pd.DataFrame(exclude_tags)
    if len(tag_df.columns) == 1:
        q = """
SELECT DISTINCT s.domain
FROM UNNEST(:tag_names) AS t(name)
    JOIN site_tag AS st ON st.name=t.name
    JOIN ass_site_site_tag AS ast ON ast.site_tag_id=st.id
    JOIN site AS s ON s.id=ast.site_id
"""
        rs = engine.execute(text(q).bindparams(tag_names=exclude_tags))
    elif len(tag_df.columns) == 2:
        tag_df.columns = ['tag_source', 'tag_name']
        q = """
SELECT DISTINCT s.domain
FROM UNNEST(:tag_sources, :tag_names) AS t(source, name)
    JOIN site_tag AS st ON st.source=t.source AND st.name=t.name
    JOIN ass_site_site_tag AS ast ON ast.site_tag_id=st.id
    JOIN site AS s ON s.id=ast.site_id
"""
        rs = engine.execute(
            text(q).bindparams(
                tag_sources=tag_df.tag_source.tolist(),
                tag_names=tag_df.tag_name.tolist()))
    else:
        raise TypeError('Invalid excluded tags format!')
    return df.loc[~df.domain.isin([r[0] for r in rs])]


def attach_site_tags(engine, df):
    if len(df) < 1:
        return df
    if 'domain' not in df:
        raise ValueError('`domain` column should exist in `df`')
    q = """
    SELECT t.domain,
        JSON_AGG(JSON_BUILD_OBJECT('name', st.name, 'source', st.source))
            AS site_tags
    FROM UNNEST(:domains) AS t(domain)
        JOIN site AS s ON s.domain=t.domain
        JOIN ass_site_site_tag AS ast ON ast.site_id=s.id
        JOIN site_tag AS st ON st.id=ast.site_tag_id
    GROUP BY t.domain
    """
    rs = engine.execute(text(q).\
                        bindparams(domains=df.domain.unique().tolist()))
    df2 = pd.DataFrame(iter(rs), columns=rs.keys())
    df = pd.merge(df, df2, how='left', on='domain')
    return df


def db_query_twitter_shares(engine, df):
    """Query the number of tweets sharing the articles.

    Parameters
    ----------
    engine : object
        A SQLAlchemy connection, e.g., engine or session.
    df : pd.DataFrame
        A dataframe contains one column named 'id', which representing the
        the `group_id` of articles.

    Returns
    -------
    pandas.DataFrame
        One new column named 'number_of_tweets' is added to the input
        DataFrame `df`.
    """
    if len(df) == 0:
        return df
    q = """
    SELECT t.group_id AS id,
        COUNT(DISTINCT atu.tweet_id) AS number_of_tweets
    FROM UNNEST(:ids) AS t(group_id)
        LEFT JOIN article AS a ON a.group_id=t.group_id
        LEFT JOIN url AS u ON u.article_id=a.id
        LEFT JOIN ass_tweet_url AS atu ON atu.url_id=u.id
    GROUP BY t.group_id
    """
    rs = engine.execution_options(stream_results=True)\
        .execute(text(q), ids=df['id'].tolist())
    df1 = pd.DataFrame(iter(rs), columns=rs.keys())
    df = pd.merge(df, df1, on='id', how='inner', sort=False)
    return df


def db_query_article(engine, ids):
    """Query articles that having group_id equals id.

    Parameters
    ----------
    engine : object
        A SQLAlchemy connection, e.g., engine or session.
    ids : list
        A list of group_id.

    Returns
    -------
    pandas.DataFrame
        Columns of the dataframe are ['id', 'canonical_url',
        'title', 'date_published', 'domain', 'site_type']
    """
    q = """
    SELECT DISTINCT ON (a.group_id)
                a.group_id AS id,
                a.canonical_url AS canonical_url,
                a.title AS title,
                coalesce(a.date_published, a.date_captured) AS date_published,
                s.domain AS domain,
                s.site_type AS site_type
    FROM UNNEST(:gids) AS t(group_id)
        JOIN article AS a ON a.group_id=t.group_id
        JOIN site AS s ON s.id=a.site_id
    ORDER BY a.group_id, a.date_captured
    """
    rs = engine.execution_options(stream_results=True)\
        .execute(text(q), gids=ids)
    return pd.DataFrame(iter(rs), columns=rs.keys())


def db_query_latest_articles(engine,
                             past_hours=2,
                             domains=None,
                             domains_file=None):
    """Query the latest collected articles from database.

    Parameters
    ----------
    engine : object
        A SQLAlchemy connection, e.g., engine or session.
    past_hours : int
        Set the hours from now to past to be defined as latest hours.
    domains : object
        If None, return all articles in the latest hours;
        If str, should be one of {'fact_checking', 'claim', 'fake'}:
            if 'fact_checking', return fact checking articles,
            if 'claim', return claim articles,
            if 'fake', return selected fake articles, which is a subset of
               claim, which is selected by us.
        If array of domain, return articles belonging to these domains.
    domains_file : str
        When `domains` is 'fake', the actual used domains are loaded from
        file `domains_file`. If this file doesn't exist, then `claim` type
        domains would be used.

    Returns:
    --------
    pandas.DataFrame
        Columns of the dataframe are ['id', 'date_captured',
        'title', 'canonical_url', 'site_type', 'number_of_tweets'].
    """
    q1 = """
    WITH domain AS (
        SELECT * FROM UNNEST(:domains) AS t(name)
    ),
    chosen_site AS(
        SELECT DISTINCT site.id AS site_id
        FROM site
        JOIN domain ON domain.name=site.domain
        UNION
        SELECT DISTINCT ad.site_id AS site_id
        FROM alternate_domain AS ad
        JOIN domain ON domain.name=ad.name
    )
    SELECT DISTINCT ON (a.group_id) a.group_id AS id,
        a.canonical_url AS canonical_url,
        a.title AS title,
        a.date_captured AS date_published,
        s.domain AS domain,
        s.site_type AS site_type
    FROM article AS a
        JOIN site AS s ON s.id=a.site_id
        JOIN chosen_site AS cs ON cs.site_id=s.id
    WHERE a.date_captured>=:latest
        AND a.canonical_url SIMILAR TO 'https?://[^/]+/_%'
        {where_condition}
    ORDER BY a.group_id, a.date_captured
    """
    q2 = """
    SELECT DISTINCT ON (a.group_id) a.group_id AS id,
        a.canonical_url AS canonical_url,
        a.title AS title,
        a.date_captured AS date_published,
        s.domain AS domain,
        s.site_type AS site_type
    FROM article AS a
        JOIN site AS s ON s.id=a.site_id
    WHERE a.date_captured>=:latest
        AND a.canonical_url SIMILAR TO 'https?://[^/]+/_%'
        {where_condition}
    ORDER BY a.group_id, a.date_captured
    """
    latest = datetime.utcnow() - timedelta(hours=past_hours)
    where_condition = ''
    if domains == 'fact_checking':
        where_condition += " AND s.site_type LIKE 'fact_checking' "
    elif domains == 'claim':
        where_condition += " AND s.site_type LIKE 'claim' "
    elif domains == 'fake':
        if domains_file is None:
            logger.warning('No domains_file provide! Using \'claim\' instead')
            where_condition += " AND s.site_type LIKE 'claim' "
        elif not isfile(domains_file):
            logger.warning('File %s not found! Using \'claim\' instead',
                           domains_file)
            where_condition += " AND s.site_type LIKE 'claim' "
        else:
            with open(domains_file) as f:
                domains = f.readlines()
            domains = [x.strip() for x in domains]
            domains = [x for x in domains if len(x) > 0]
    if isinstance(domains, (list, tuple)) is True:
        q = text(q1.format(where_condition=where_condition))\
                .bindparams(domains=domains)
    else:
        q = text(q2.format(where_condition=where_condition))
    q = q.bindparams(latest=latest)
    rs = engine.execute(q)
    df = pd.DataFrame(iter(rs), columns=rs.keys())
    return attach_site_tags(engine, df)


def db_query_tweets(engine, ids):
    """Query tweets that sharing articles with group_id equals ids.

    Parameters
    ----------
    engine : object
        A SQLAlchemy connection, e.g., engine or session.
    ids : list
        A list of group_id.

    Returns
    -------
    pandas.DataFrame
        Columns of the dataframe are ['tweet_id',
        'tweet_created_at', 'title', 'site_type', 'id',
        'domain', 'date_published', 'canonical_url']
    """
    df = db_query_article(engine, ids)
    if len(df) == 0:
        return df
    q = """
    SELECT DISTINCT CAST(tw.raw_id AS text) AS tweet_id,
                    tw.created_at AS tweet_created_at,
                    t.group_id AS id
    FROM UNNEST(:gids) AS t(group_id)
        JOIN article AS a ON a.group_id=t.group_id
        JOIN url AS u ON u.article_id=a.id
        JOIN ass_tweet_url AS atu ON atu.url_id=u.id
        JOIN tweet AS tw ON tw.id=atu.tweet_id
    """
    rs = engine.execution_options(stream_results=True)\
        .execute(text(q), gids=ids)
    df2 = pd.DataFrame(iter(rs), columns=rs.keys())
    df = pd.merge(df, df2, on='id', how='inner', sort=False)
    df = df.sort_values('date_published', ascending=True)
    return df


def edge_iter(iter, user_map, include_user_mentions=True):
    """Build edge.

    Parameters
    ----------
    iter : iterable
        The iterable object, SQLAlchemy search results.
    user_map : dict
        A dict to keep tract the user_id and user_screen_name.
    include_user_mentions : bool
        Whether include user mentions.

    Returns
    -------
    iterable
        The element of this iterable is a tuple.
    """
    for gid, tw_id, tw_created_at, tw_uid, tw_sn, re_uid, re_sn,\
            qu_uid, qu_sn, ir_uid, ir_sn, quoted_urls,\
            user_mentions, url_id, url_raw in iter:
        from_uid = None
        to_uid = None
        tweet_type = 'origin'
        # retweet
        if re_uid is not None:
            from_uid = re_uid
            from_sn = re_sn
            to_uid = tw_uid
            to_sn = tw_sn
            tweet_type = 'retweet'
        # reply
        elif ir_uid is not None:
            from_uid = tw_uid
            from_sn = tw_sn
            to_uid = ir_uid
            to_sn = ir_sn
            tweet_type = 'reply'
        # quote
        elif qu_uid:
            tweet_type = 'quote'
            to_uid = tw_uid
            to_sn = tw_sn
            # test url in quoted_status
            for quoted_url in quoted_urls:
                if url_raw == quoted_url['expanded_url']:
                    from_uid = qu_uid
                    from_sn = qu_sn
                    break

        if from_uid is not None and to_uid is not None:
            user_map[from_uid] = from_sn
            user_map[to_uid] = to_sn
            yield (gid, tw_id, tw_created_at, from_uid, to_uid, False,
                   tweet_type, url_id)
        # mentions
        # include_user_mentions is set
        # user_mentions of current tweet is not empty
        # current tweet is not a retweet
        if include_user_mentions is True\
                and user_mentions\
                and tweet_type != 'retweet':
            user_map[tw_uid] = tw_sn
            for user in user_mentions:
                try:
                    m_to_uid = user['id_str']
                    m_to_sn = user['screen_name']
                    user_map[m_to_uid] = m_to_sn
                    if tweet_type == 'reply':
                        # exclude reply uid
                        if m_to_uid != ir_uid:
                            yield (gid, tw_id, tw_created_at, tw_uid, m_to_uid,
                                   True, tweet_type, url_id)
                    elif tweet_type == 'quote':
                        # exclude quoted user
                        if m_to_uid != qu_uid:
                            yield (gid, tw_id, tw_created_at, tw_uid, m_to_uid,
                                   True, tweet_type, url_id)
                    else:
                        # this is origin tweet, include all mentions
                        yield (gid, tw_id, tw_created_at, tw_uid, m_to_uid,
                               True, tweet_type, url_id)
                except KeyError as e:
                    logger.error(e)


def limit_by_k_core(df, nodes_limit, edges_limit):
    """Use k_core method to remove less import nodes and edges.

    Parameters
    ----------
    df : pandas.DataFrame
        The edges dataframe.
    nodes_limit : int
        The maximum number of nodes to return.
    edges_limit : int
        The maximum number of edges to return.

    Returns
    -------
    pandas.DataFrame
        This dataframe is refined with k_core algorithm.
    """
    v_cols = ['from_user_id', 'to_user_id']
    G = nx.from_pandas_dataframe(
        df, v_cols[0], v_cols[1], create_using=nx.DiGraph())
    G.remove_edges_from(G.selfloop_edges())
    #
    # sort nodes by ascending core number
    core = nx.core_number(G)
    nodes_list = sorted(core.items(), key=lambda k: k[1], reverse=False)
    nodes_list = list(zip(*nodes_list))[0]
    nodes_list = list(nodes_list)
    #
    # if there are no nodes in excess, do not execute
    excess_nodes = G.number_of_nodes() - nodes_limit
    if nodes_limit and excess_nodes > 0:
        nodes_to_remove = nodes_list[:excess_nodes]
        nodes_list = nodes_list[excess_nodes:]
        G.remove_nodes_from(nodes_to_remove)
    #
    # remove nodes in batches until the the number of edges is below the
    # limit. Only execute if edges_limit argument is passed (not None) and
    # is positive
    if edges_limit:
        batch_size = 10
        while G.number_of_edges() > edges_limit:
            nodes_to_remove = nodes_list[:batch_size]
            nodes_list = nodes_list[batch_size:]
            G.remove_nodes_from(nodes_to_remove)
    logger.debug('filtered nodes/edges = %s/%s',
                 G.number_of_nodes(), G.number_of_edges())
    df = df.set_index(['from_user_id', 'to_user_id'])
    df = df.loc[G.edges()]
    return df.reset_index()


def db_query_network_old(engine,
                         ids,
                         nodes_limit=1000,
                         edges_limit=12500,
                         include_user_mentions=True):
    """Query the diffusion network that shares articles with group_id as `ids`.

    Parameters
    ----------
    engine : object
        A SQLAlchemy connection, e.g., engine or session.
    ids : list
        A list of group_id.
    nodes_limit : int
        The maximum number of nodes to return.
    edges_limit : int
        The maximum number of edges to return.
    include_user_mentions : bool
        Whether include user mentions.

    Returns
    -------
    pandas.DataFrame
        Columns of the dataframe are ['from_user_id',
        'from_user_screen_name', 'to_user_id', 'to_user_screen_name',
        'tweet_id', 'tweet_created_at', 'tweet_type', 'is_mention',
        'id', 'title', 'domain', 'canonical_url', 'date_published',
        'site_type', 'url_id']
    """
    df = db_query_article(engine, ids)
    if len(df) == 0:
        return df
    q = """
    SELECT t.group_id, CAST(tw.raw_id AS text), tw.created_at,
        tw.json_data#>>'{user, id}' AS tw_uid,
        tw.json_data#>>'{user, screen_name}' AS tw_sn,
        tw.json_data#>>'{retweeted_status, user, id}' AS re_uid,
        tw.json_data#>>'{retweeted_status, user, screen_name}' AS re_sn,
        tw.json_data#>>'{quoted_status, user, id}' AS qu_uid,
        tw.json_data#>>'{quoted_status, user, screen_name}' AS qu_sn,
        tw.json_data->>'in_reply_to_user_id' AS ir_uid,
        tw.json_data->>'in_reply_screen_name' AS ir_sn,
        tw.json_data#>'{quoted_status, entities, urls}' AS quote_urls,
        tw.json_data#>'{entities, user_mentions}' AS tw_user_m,
        u.id, u.raw
    FROM UNNEST(:gids) AS t(group_id)
        JOIN article AS a ON a.group_id=t.group_id
        JOIN url AS u ON u.article_id=a.id
        JOIN ass_tweet_url AS atu ON atu.url_id=u.id
        JOIN tweet AS tw ON tw.id=atu.tweet_id
    ORDER BY tw.created_at ASC
    """
    user_map = dict()
    rs = engine.execution_options(stream_results=True)\
        .execute(text(q), gids=ids)
    df2 = pd.DataFrame(
        edge_iter(rs, user_map, include_user_mentions),
        columns=[
            'id', 'tweet_id', 'tweet_created_at', 'from_user_id', 'to_user_id',
            'is_mention', 'tweet_type', 'url_id'
        ])
    if len(user_map) == 0 or len(df2) == 0:
        return pd.DataFrame()
    df3 = pd.DataFrame.from_dict(user_map, orient='index')
    df3.columns = ['screen_name']
    df2 = pd.merge(
        df2, df3, how='left', left_on='from_user_id', right_index=True)
    df2.rename(inplace=True, columns=dict(screen_name='from_user_screen_name'))
    df2 = pd.merge(df2, df3, how='left', left_on='to_user_id', right_index=True)
    df2.rename(inplace=True, columns=dict(screen_name='to_user_screen_name'))
    df = pd.merge(df, df2, on='id', how='inner', sort=False)
    df = df.sort_values('date_published', ascending=True)
    return limit_by_k_core(df, nodes_limit, edges_limit)


def db_query_network(engine,
                     ids,
                     nodes_limit=1000,
                     edges_limit=12500,
                     include_user_mentions=True):
    """Query the diffusion network that shares articles with group_id as `ids`.

    Parameters
    ----------
    engine : object
        A SQLAlchemy connection, e.g., engine or session.
    ids : list
        A list of group_id.
    nodes_limit : int
        The maximum number of nodes to return.
    edges_limit : int
        The maximum number of edges to return.
    include_user_mentions : bool
        Whether include user mentions.

    Returns
    -------
    pandas.DataFrame
        Columns of the dataframe are ['from_user_id',
        'from_user_screen_name', 'to_user_id', 'to_user_screen_name',
        'tweet_id', 'tweet_created_at', 'tweet_type', 'is_mention',
        'id', 'title', 'domain', 'canonical_url', 'date_published',
        'site_type', 'url_id']
    """
    df = db_query_article(engine, ids)
    if len(df) == 0:
        return df
    q = """
    SELECT DISTINCT t.group_id, tw.raw_id::text, tw.created_at,
                tne.from_raw_id::text, tne.to_raw_id::text,
                tuu1.screen_name AS from_user_screen_name,
                tuu2.screen_name AS to_user_screen_name,
                tne.is_mention, tne.tweet_type, u.id AS url_id
    FROM UNNEST(:gids) AS t(group_id)
        JOIN article AS a ON a.group_id=t.group_id
        JOIN url AS u ON u.article_id=a.id
        JOIN ass_tweet_url AS atu ON atu.url_id=u.id
        JOIN tweet AS tw ON tw.id=atu.tweet_id
        JOIN twitter_network_edge AS tne ON tne.tweet_raw_id=tw.raw_id
        JOIN twitter_user_union AS tuu1 ON tuu1.raw_id=tne.from_raw_id
        JOIN twitter_user_union AS tuu2 ON tuu2.raw_id=tne.to_raw_id
    """
    rs = engine.execution_options(stream_results=True)\
        .execute(text(q), gids=ids)
    df2 = pd.DataFrame(
        iter(rs),
        columns=[
            'id', 'tweet_id', 'tweet_created_at', 'from_user_id', 'to_user_id',
            'from_user_screen_name', 'to_user_screen_name', 'is_mention',
            'tweet_type', 'url_id'
        ])
    if len(df2) == 0:
        return pd.DataFrame()
    df = pd.merge(df, df2, on='id', how='inner', sort=False)
    df = df.sort_values('date_published', ascending=True)
    return limit_by_k_core(df, nodes_limit, edges_limit)


def db_query_top_spreaders(engine, upper_day, most_recent=False):
    """Query top 20 spreaders in the 30 days window.

    Parameters
    ----------
    engine : object
        A SQLAlchemy connection, e.g., engine or session.
    upper_day : datetime
        The right edge of the 30 days window.
    most_recent : bool
        When no results for the `upper_day`, whether result the
        most recent available results.

    Returns
    -------
    pandas.DataFrame
        Columns of the dataframe are ['upper_day', 'user_id',
        'user_raw_id', 'user_screen_name', 'site_type',
        'spreading_type', 'number_of_tweets', 'bot_score']
    """
    q0 = """
    SELECT upper_day, user_id, user_raw_id, user_screen_name, site_type,
    spreading_type, number_of_tweets, bot_or_not
    FROM top20_spreader_monthly WHERE upper_day=:upper_day
    ORDER BY site_type, spreading_type, number_of_tweets DESC
    """
    q = text(q0).bindparams(upper_day=upper_day)
    rp = engine.execute(q)
    df = pd.DataFrame(iter(rp), columns=rp.keys())
    if len(df) == 0 and most_recent is True:
        session = Session()
        upper_day = get_max(session, Top20SpreaderMonthly.upper_day)
        if upper_day is None:
            raise APINoResultError
        else:
            q = text(q0).bindparams(upper_day=upper_day)
            rp = engine.execute(q)
            df = pd.DataFrame(iter(rp), columns=rp.keys())
    df['user_raw_id'] = df.user_raw_id.astype(str)

    def get_bot_score(bon):
        if bon is None:
            return None
        elif 'score' in bon:
            return bon['score']
        elif 'scores' in bon:
            return bon['scores'].get('universal')
        else:
            return None

    df['bot_score'] = df.bot_or_not.apply(get_bot_score)
    df = df.drop('bot_or_not', axis=1)
    return df


def db_query_top_articles(engine, upper_day, most_recent=False,
                          exclude_tags=[]):
    """Query top 20 articles in the 30 days window.

    Parameters
    ----------
    engine : object
        A SQLAlchemy connection, e.g., engine or session.
    upper_day : datetime
        The right edge of the 30 days window.
    most_recent : bool
        When no results for the `upper_day`, whether result the
        most recent available results.

    Returns
    -------
    pandas.DataFrame
        Columns of the dataframe are ['upper_day', 'date_captured',
        'title', 'canonical_url', 'site_type', 'number_of_tweets'].
    """
    q0 = """
    SELECT upper_day, date_captured, title, canonical_url, site_type,
    number_of_tweets
    FROM top20_article_monthly WHERE upper_day=:upper_day
    ORDER BY site_type, number_of_tweets DESC
    """
    q = text(q0).bindparams(upper_day=upper_day)
    rp = engine.execute(q)
    df = pd.DataFrame(iter(rp), columns=rp.keys())
    if len(df) == 0 and most_recent is True:
        session = Session()
        upper_day = get_max(session, Top20ArticleMonthly.upper_day)
        if upper_day is None:
            raise APINoResultError
        else:
            q = text(q0).bindparams(upper_day=upper_day)
            rp = engine.execute(q)
            df = pd.DataFrame(iter(rp), columns=rp.keys())
    if exclude_tags:
        return db_query_filter_tags(engine, df, exclude_tags)
