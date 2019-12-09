# -*- coding: utf-8 -*-
"""Implement parsers to parse tweet, savers to save the parsed tweet into
database.
    1) Basically, a tweet would first parse into tuples according to the
       associated tables described in hoaxy.database.models. The main
       difference is that the tuples keep the original pieces of tweet
       without considering of foreign keys (because at the parsing stage,
       we have not saving each part yet).
    2) When saving, objects of the independent tables (referred as foreign key
       by other table) would be saved first, then the actual `id`s of the
       objects are known, and we should update the parsed pieces when needed.
       For example, the orginal piece for `ass_tweet_url` is
       tuple(tweet_raw_id, url), after parsed objects of `tweet` and `url`
       table are saved, we need to map the tuple(tweet_raw_id, url) in
       order to save it.
    3) We implement a BulkSaver class for the purpose of bulk insertion.
"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

import logging
from functools import reduce
from operator import iconcat

import pandas as pd
import simplejson as json
from pathos.multiprocessing import ProcessPool
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

from hoaxy.database.models import (
    MAX_URL_LEN, AssTweet, AssTweetHashtag, AssTweetUrl, AssUrlPlatform,
    Hashtag, Tweet, TwitterNetworkEdge, TwitterUser, TwitterUserUnion, Url)
from hoaxy.utils.dt import utc_from_str

logger = logging.getLogger(__name__)

#
# Meta info for each pieces of parsed tweet.
# p_keys are used to keep the parsed fields
# d_keys are the actual columns saved in the datbase
# pu_keys are used for p_keys to drop duplications
# du_keys are used as database table unique constraint
# They shoud be consistent with the database models.

PMETA = dict(
    #
    # independent tables, with no foreign keys
    # p_keys and d_keys should be the same
    #
    url=dict(p_keys=['raw'], d_keys=['raw'], pu_keys=['raw'], du_keys=['raw']),
    hashtag=dict(
        p_keys=['text'], d_keys=['raw'], pu_keys=['text'], du_keys=['text']),
    twitter_user=dict(
        p_keys=['raw_id'],
        d_keys=['raw_id'],
        pu_keys=['raw_id'],
        du_keys=['raw_id']),
    # both mentioned_user and full_user are belong to twitter_user_union
    # their insertion is slightly different
    mentioned_user=dict(
        p_keys=['raw_id', 'screen_name', 'updated_at'],
        d_keys=['raw_id', 'screen_name', 'updated_at'],
        pu_keys=['raw_id'],
        du_keys=['raw_id']),
    full_user=dict(
        p_keys=[
            'raw_id', 'screen_name', 'followers_count', 'profile', 'updated_at'
        ],
        d_keys=[
            'raw_id', 'screen_name', 'followers_count', 'profile', 'updated_at'
        ],
        pu_keys=['raw_id'],
        du_keys=['raw_id']),
    #
    # tables with foreign keys
    # p_keys shoud be mapped to d_keys when doing pandas merge operation
    #

    # for `tweet` table, instead of using 'raw_id', we use 'tweet_raw_id' in
    # order to be seperated from 'user_raw_id'. However, before database
    # operations 'tweet_raw_id' must be renamed to 'raw_id'
    tweet=dict(
        p_keys=['raw_id', 'json_data', 'user_raw_id', 'created_at'],
        d_keys=['raw_id', 'json_data', 'user_id', 'created_at'],
        pu_keys=['raw_id'],
        du_keys=['raw_id'],
    ),
    ass_tweet=dict(
        p_keys=[
            'tweet_raw_id', 'retweeted_status_id', 'quoted_status_id',
            'in_reply_to_status_id'
        ],
        d_keys=[
            'id', 'retweeted_status_id', 'quoted_status_id',
            'in_reply_to_status_id'
        ],
        pu_keys=['tweet_raw_id'],
        du_keys=['id']),
    ass_tweet_url=dict(
        p_keys=['tweet_raw_id', 'url_raw'],
        d_keys=['tweet_id', 'url_id'],
        pu_keys=['tweet_raw_id', 'url_raw'],
        du_keys=['tweet_id', 'url_id']),
    ass_tweet_hashtag=dict(
        p_keys=['tweet_raw_id', 'hashtag_text'],
        d_keys=['tweet_id', 'hashtag_id'],
        pu_keys=['tweet_raw_id', 'hashtag_text'],
        du_keys=['tweet_id', 'hashtag_id']),
    twitter_network_edge=dict(
        p_keys=[
            'tweet_raw_id', 'from_raw_id', 'to_raw_id', 'url_raw',
            'is_quoted_url', 'is_mention', 'tweet_type'
        ],
        d_keys=[
            'tweet_raw_id', 'from_raw_id', 'to_raw_id', 'url_id',
            'is_quoted_url', 'is_mention', 'tweet_type'
        ],
        pu_keys=[
            'tweet_raw_id', 'from_raw_id', 'to_raw_id', 'url_raw',
            'is_quoted_url', 'is_mention', 'tweet_type'
        ],
        du_keys=[
            'tweet_raw_id', 'from_raw_id', 'to_raw_id', 'url_id',
            'is_quoted_url', 'is_mention', 'tweet_type'
        ],
    ))


def replace_null_byte(jd, fp=None, new=''):
    """Find and delete NULL bytes in a JSON string dumped from a JSON object.

    We have experienced DataError exception when inserting the tweet JSON
    string caused by NULL bytes r'\u0000'. This function would replace the
    NULL byte with pre-defined char, e.g., default is empty (that is delete
    the NULL byte).

    Parameters
    ----------
    jd : object
        A JSON object (dict).
    Returns
    -------
    A JSON string.
    """
    if jd is None:
        return jd
    data = json.dumps(jd, encoding='utf-8')
    if r'\u0000' in data:
        logger.warning(r'NULL byte (\u0000) found in %r and deleted!',
                       jd['id'])
        if fp is not None:
            fp.write(jd['id_str'])
            fp.write('\n')
        data = data.replace(r'\\u0000', new)
        return json.loads(data, encoding='utf-8')
    else:
        return jd


class Parser():
    """This class implement functions to parse a tweet into pieces of meta
    data that can populate the database tables accordingly.
    """

    def __init__(self, save_none_url_tweet=False):
        """Constructor of Parser.
        save_none_url_tweet : Boolean
            Whether to save the tweet if it contains no URLs.
        """
        #
        # store all urls, mentions, hashtags we met. The dict should mark
        # where the element comes from,
        # e.g., dict(this=set(), retweet=set(), ...)
        #
        self.save_none_url_tweet = save_none_url_tweet
        self.urls = dict(this=set(), quote=set(), retweet=set(), union=set())
        self.mentions = dict(
            this=set(), quote=set(), retweet=set(), union=set())
        self.hashtags = dict(
            this=set(), quote=set(), retweet=set(), union=set())
        #
        # full_user is a list, unique key is user_raw_id, not the whole tuple
        self.full_user = list()
        self.in_reply_to_user = None
        self.edges = set()
        self.ass_tweet = None
        self.created_at = None

    def _parse_entities(self, entities, label):
        """Internal function to briefly parse the entities in a tweet object.

        By default postgresql column is case-sensitive and URL itself may
        be case sensitive, no need to convert case. However, for hashtags,
        twitter use them as case-insensitive, so we need to convert them
        into lower case.

        Parameters
        ----------
        entities: JSON object
            The entities field of a tweet object.
        label: string
            The label of the entities, unavailable values are:
              'this', current tweet;
              'retweet', the retweeted_status of this tweet;
              'quote', the quoted_status of this tweet;
              'union', the whole tweet
        """
        if 'urls' in entities:
            for url in entities['urls']:
                u = url.get('expanded_url')
                if u and len(u) <= MAX_URL_LEN:
                    self.urls[label].add(u)
                    self.urls['union'].add(u)
                else:
                    logger.error('Invalid URL %s', u)
        if 'user_mentions' in entities:
            for m in entities['user_mentions']:
                user_raw_id = m.get('id')
                screen_name = m.get('screen_name')
                if user_raw_id and screen_name:
                    self.mentions[label].add((user_raw_id, screen_name))
                    self.mentions['union'].add((user_raw_id, screen_name))
        # hashtag in twitter is case insensitive, shall be converted
        if 'hashtags' in entities:
            for h in entities['hashtags']:
                htext = h.get('text')
                if htext:
                    htext = htext.lower()
                    self.hashtags[label].add(htext)
                    self.hashtags['union'].add(htext)

    def _parse_l1(self, jd):
        """First level parsing that collects and parses all associated entities.

        Parameters
        ----------
        jd: JSON
            A tweet JSON object.

        Returns
        -------
        Returns nothing. The class members self.urls, self.mentions and
        self.hashtags would be populated.
        """
        # make sure these members are flushed as new
        self.urls = dict(this=set(), quote=set(), retweet=set(), union=set())
        self.mentions = dict(
            this=set(), quote=set(), retweet=set(), union=set())
        self.hashtags = dict(
            this=set(), quote=set(), retweet=set(), union=set())
        # this status
        if 'entities' in jd:
            self._parse_entities(jd['entities'], 'this')
        if 'retweeted_status' in jd and 'entities' in jd['retweeted_status']:
            self._parse_entities(jd['retweeted_status']['entities'], 'retweet')
        if 'quoted_status' in jd and 'entities' in jd['quoted_status']:
            self._parse_entities(jd['quoted_status']['entities'], 'quote')

    def _parse_l2(self, jd):
        """Second Level parsing, the main function is to build parsed objects
        for the most complicate table:

            twitter_network_edge

        Parameters
        ---------
        jd: JSON
            A tweet JSON object.
        """
        #
        # Make sure up-using class members are flushed
        #
        self.full_user = list()
        self.edges = set()

        # start parsing
        tweet_raw_id = jd['id']
        user_raw_id = jd['user']['id']
        user_screen_name = jd['user']['screen_name']
        self.created_at = utc_from_str(jd['created_at'])
        # add this user as full_user
        self.full_user.append(
            (user_raw_id, user_screen_name, jd['user']['followers_count'],
             jd['user'], self.created_at))
        quoted_status_id = None
        retweeted_status_id = None
        if 'quoted_status' in jd:
            quoted_user_id = jd['quoted_status']['user']['id']
            quoted_screen_name = jd['quoted_status']['user']['screen_name']
            quoted_status_id = jd['quoted_status']['id']
            self.full_user.append(
                (quoted_user_id, quoted_screen_name,
                 jd['quoted_status']['user']['followers_count'],
                 jd['quoted_status']['user'], self.created_at))
        if 'retweeted_status' in jd:
            retweeted_user_id = jd['retweeted_status']['user']['id']
            retweeted_screen_name = jd['retweeted_status']['user'][
                'screen_name']
            retweeted_status_id = jd['retweeted_status']['id']
            self.full_user.append(
                (retweeted_user_id, retweeted_screen_name,
                 jd['retweeted_status']['user']['followers_count'],
                 jd['retweeted_status']['user'], self.created_at))
        in_reply_to_status_id = jd['in_reply_to_status_id']
        in_reply_to_user_id = jd['in_reply_to_user_id']
        in_reply_to_screen_name = jd['in_reply_to_screen_name']
        if in_reply_to_user_id is not None and\
                in_reply_to_screen_name is not None:
            self.in_reply_to_user = (in_reply_to_user_id,
                                     in_reply_to_screen_name)
        self.ass_tweet = (tweet_raw_id, retweeted_status_id, quoted_status_id,
                          in_reply_to_status_id)
        #
        # Building edges
        #

        # 2-1) retweet, focusing on retweeted_status
        #               edge direction: from retweeted_user to current user
        if retweeted_status_id is not None:
            logger.debug('2-1-a) building edges for retweet=%s', tweet_raw_id)
            for u in self.urls['retweet']:
                self.edges.add((tweet_raw_id, retweeted_user_id, user_raw_id,
                                u, False, False, 'retweet'))
        # 2-2) reply, focusing on current status
        #             edges direction: from current user to mentions
        if in_reply_to_status_id is not None:
            logger.debug('2-1-b) building edges for reply=%s', tweet_raw_id)
            # in_reply_to_user, edge
            for url in self.urls['this']:
                self.edges.add((tweet_raw_id, user_raw_id, in_reply_to_user_id,
                                url, False, False, 'reply'))
            # mentions, edges
            for mention_id, mention_screen_name in self.mentions['this']:
                if mention_id != in_reply_to_user_id:
                    for url in self.urls['this']:
                        self.edges.add((tweet_raw_id, user_raw_id, mention_id,
                                        url, False, True, 'reply'))
        # 2-3) quote
        if quoted_status_id is not None:
            # 2-3-1) retweeted quote, focusing on quoted_status
            #                         treated as retweet edge
            if retweeted_status_id is not None:
                logger.debug(
                    '2-1-c) building edges for the quote of a retweet=%s',
                    tweet_raw_id)
                for url in self.urls['quote']:
                    self.edges.add((tweet_raw_id, retweeted_user_id,
                                    user_raw_id, url, True, False, 'retweet'))
            # 2-3-2) replied quote, focusing on quoted_status
            #                       treated as reply edge
            elif in_reply_to_status_id is not None:
                logger.debug(
                    '2-1-c) building edges for the quote of a reply=%s',
                    tweet_raw_id)
                # in_reply_to_user, edges for quoted url
                for url in self.urls['quote']:
                    self.edges.add(
                        (tweet_raw_id, user_raw_id, in_reply_to_user_id, url,
                         True, False, 'reply'))
                # mentions, edges for quoted url
                for mention_id, mention_screen_name in self.mentions['this']:
                    if mention_id != in_reply_to_user_id:
                        for url in self.urls['quote']:
                            self.edges.add(
                                (tweet_raw_id, user_raw_id, mention_id, url,
                                 True, True, 'reply'))
            # 2-3-3) pure quote
            else:
                logger.debug('2-1-c) Building edges for a pure quote=%s',
                             tweet_raw_id)
                # a. information edges: from quoted_user to this_user
                #                       for urls inputted by quoted user
                for url in self.urls['quote']:
                    self.edges.add((tweet_raw_id, quoted_user_id, user_raw_id,
                                    url, True, False, 'quote'))
                # b. information edges: from this_user to mentioned_users
                #                       of this_user
                #                       for both urls inputted by this user
                #                       and quoted_user
                for mention_id, mention_screen_name in self.mentions['this']:
                    for url in self.urls['quote']:
                        self.edges.add((tweet_raw_id, user_raw_id, mention_id,
                                        url, True, True, 'quote'))
                    for url in self.urls['this']:
                        self.edges.add((tweet_raw_id, user_raw_id, mention_id,
                                        url, False, True, 'quote'))
        # 2-4) original tweet
        if retweeted_status_id is None and in_reply_to_status_id is None\
                and quoted_status_id is None:
            logger.debug('2-1-d) building edges for original tweet=%s',
                         tweet_raw_id)
            for mention_id, mention_screen in self.mentions['this']:
                for url in self.urls['this']:
                    self.edges.add((tweet_raw_id, user_raw_id, mention_id, url,
                                    False, True, 'origin'))

    def parse_one(self, jd, validate_tweet=True):
        """The main parse function.

        Parameters
        ----------
        jd : json
            Tweet json data.

        validate_tweet : Boolean
            To ensure that no null bytes in the tweet string. Default is True.

        Returns
        ----------
        A dict contains all necessary parsed objects.
        """
        if validate_tweet is True:
            jd = replace_null_byte(jd)
        self._parse_l1(jd)
        if self.save_none_url_tweet is False and not self.urls['union']:
            logger.warning('Tweet=%s with no URL, ignored', jd.get('id'))
            return None
        self._parse_l2(jd)
        tweet_raw_id = jd['id']
        user_raw_id = jd['user']['id']
        mentioned_user = [
            m + (self.created_at, ) for m in self.mentions['union']
        ]
        if self.in_reply_to_user is not None\
                and self.in_reply_to_user not in self.mentions['union']:
            mentioned_user.append(self.in_reply_to_user + (self.created_at, ))
        result = dict(
            # Table ass_tweet_url: tweet_id, url_id
            ass_tweet_url=[(tweet_raw_id, url) for url in self.urls['union']],
            # Table ass_tweet_hashtag: tweet_id, hashtag_id
            ass_tweet_hashtag=[(tweet_raw_id, hashtag)
                               for hashtag in self.hashtags['union']],
            # Table url: raw
            url=[(url, ) for url in self.urls['union']],
            # Table hashtag: text
            hashtag=[(hashtag, ) for hashtag in self.hashtags['union']],
            # Table tweet: raw_id, json_data, user_id
            tweet=[(tweet_raw_id, jd, user_raw_id, self.created_at)],
            # Table ass_tweet: tweet_id, retweeted_status_id, quoted_status_id
            # in_reply_to_status_id
            ass_tweet=[self.ass_tweet],
            # Table twitter_user: raw_id
            twitter_user=[(user_raw_id, )],
            # Table twitter_network_edge: tweet_raw_id, from_raw_id,
            # to_raw_id, url_id, is_quoted_url, is_mention, tweet_type
            twitter_network_edge=list(self.edges),
            # Table twitter_user_union: raw_id, screen_name
            # followers_count, profile, updated_at
            # mentions and full_user are inserted into this table
            mentioned_user=mentioned_user,
            full_user=self.full_user)
        # flush class members
        self.urls = None
        self.mentions = None
        self.hashtags = None
        self.full_user = None
        self.edges = None
        self.ass_tweet = None
        return result

    def parse_many(self, jds, validate_tweet=True, multiprocesses=False):
        """Parse many tweets either in a sequencial or in parallel way.
        """
        if multiprocesses is None or multiprocesses is False\
                or multiprocesses == 0:
            results = [
                self.parse_one(jd, validate_tweet=validate_tweet) for jd in jds
            ]
        else:
            if multiprocesses is True:
                pool = ProcessPool(nodes=None)
            else:
                pool = ProcessPool(nodes=multiprocesses)
            results = pool.map(self.parse_one, jds)
        if self.save_none_url_tweet is False:
            return [r for r in results if r is not None]
        else:
            return results

    def to_dict(self, parsed_results):
        """This function standandlize the parsed results. It first reduces the
        collected parsed results into lists of tuples; then loads them as
        pandas.DataFrames whose columns represent p_keys.

        Parameters
        ----------
        parsed_results : iterable
            An iterable that contains parsed results generated by Parser.

        Returns
        ----------
        A dict, the keys of which are consistent with database tables; and
        the values of which are pandas.DataFrame.
        """
        tkeys = PMETA.keys()
        reduced_results = reduce(lambda x, y: {k: iconcat(x[k], y[k])
                                               for k in tkeys},
                                 parsed_results)
        dfs = {
            k: pd.DataFrame(reduced_results[k], columns=PMETA[k]['p_keys'])
            for k in tkeys
        }
        # drop duplicates mainly based on unique keys
        for k in tkeys:
            if k == 'full_user' or k == 'mentioned_user':
                dfs[k] = dfs[k].sort_values('updated_at', ascending=False)
            #
            # !IMPORTANT (ESPECIALLY FOR `ass_tweet` table)
            # Causion:
            # (1) The default missing values for pandas.DataFrame is
            # np.NAN, which is not compatible with SQL insertion in SQLAlchemy.
            # Thus a replace operation need to take.
            # (2) When missing values occurs, the dtype of a DataFrame would
            # be 'float' (either float32 or float64), which could truncate
            # the large numbers. Since version 24, pandas provide new data type
            # Int64 (CAPITAL I). Thus we need to convert it to this data type.
            #
            if k == 'ass_tweet':
                # replace np.NAN as None
                dfs[k] = dfs[k].astype('Int64')
                dfs[k].replace({pd.np.nan: None}, inplace=True)
            dfs[k] = dfs[k].drop_duplicates(PMETA[k]['pu_keys'], keep='first')
        return dfs

    def bulk_save(self, session, dfs, platform_id, ignore_tables=[]):
        """This function save the standandlized parsed tweet into the database
        in a bulked way.

        Parameters
        ----------
        session : Session
            Database connection session of SQLAlchemy.
        dfs : dict
            An dict that contain the standandlized parsed results, see function
            `to_dict`.
        platform_id : integer
            The `platform_id` for URL should be the id of `N_PLATFORM_TWITTER`.
        ignore_tables : list of string
            A list of table names. When inserting, we would ignore the
            insertion operation for these tables.
        Returns
        ----------
        None
        """
        #
        # insert independent tables
        #

        # Table twitter_user, url, hashtag
        for tc in [TwitterUser, Url, Hashtag]:
            tn = tc.__table__.name
            # make sure the dataframe is not empty!
            if not dfs[tn].empty and tn not in ignore_tables:
                stmt_do_nothing = insert(tc).values(
                    dfs[tn].to_dict(orient='record')).on_conflict_do_nothing(
                        index_elements=PMETA[tn]['du_keys'])
                session.execute(stmt_do_nothing)
                session.commit()

        # Table twitter_user_union
        # full_user is prioritized: try to insert these full_users, when
        # conflict do update only for those profile is NULL or newer items
        k = 'full_user'
        if not dfs[k].empty and 'twitter_user_union' not in ignore_tables:
            update_where = 'twitter_user_union.profile IS NULL OR ' + \
                'twitter_user_union.updated_at<EXCLUDED.updated_at'
            stmt = insert(TwitterUserUnion).values(
                dfs[k].to_dict(orient='record'))
            stmt = stmt.on_conflict_do_update(
                index_elements=PMETA[k]['du_keys'],
                set_=dict(
                    screen_name=stmt.excluded.screen_name,
                    followers_count=stmt.excluded.followers_count,
                    profile=stmt.excluded.profile,
                    updated_at=stmt.excluded.updated_at),
                where=text(update_where))
            session.execute(stmt)
            session.commit()
        #
        # mentioned user
        k = 'mentioned_user'
        if not dfs[k].empty and 'twitter_user_union' not in ignore_tables:
            stmt_do_nothing = insert(TwitterUserUnion).values(
                dfs[k].to_dict(orient='record')).on_conflict_do_nothing(
                    index_elements=PMETA[k]['du_keys'])
            session.execute(stmt_do_nothing)
            session.commit()

        #
        # Fetch inserted values
        #

        # Table twitter_user
        tn = 'twitter_user'
        if not dfs[k].empty:
            q = """
            SELECT tu.id AS user_id, tu.raw_id AS user_raw_id
            FROM UNNEST(:raw_ids) AS t(raw_id)
                JOIN twitter_user AS tu ON tu.raw_id=t.raw_id
            """
            rs = session.execute(
                text(q).bindparams(raw_ids=dfs[tn].raw_id.tolist()))
            df_user = pd.DataFrame(iter(rs), columns=rs.keys())
        else:
            df_user = pd.DataFrame([], columns=['user_id', 'user_raw_id'])

        # Table url
        tn = 'url'
        if not dfs[k].empty:
            q = """
            SELECT url.id AS url_id, url.raw AS url_raw
            FROM UNNEST(:raws) AS t(raw)
                JOIN url ON url.raw=t.raw
            """
            rs = session.execute(text(q).bindparams(raws=dfs[tn].raw.tolist()))
            df_url = pd.DataFrame(iter(rs), columns=rs.keys())
        else:
            df_url = pd.DataFrame([], columns=['url_id', 'url_raw'])

        # Table hashtag
        tn = 'hashtag'
        if not dfs[tn].empty:
            q = """
            SELECT hashtag.id AS hashtag_id, hashtag.text AS hashtag_text
            FROM UNNEST(:texts) AS t(text)
                JOIN hashtag ON hashtag.text=t.text
            """
            rs = session.execute(
                text(q).bindparams(texts=dfs[tn].text.tolist()))
            df_hashtag = pd.DataFrame(iter(rs), columns=rs.keys())
        else:
            df_hashtag = pd.DataFrame(
                [], columns=['hashtag_id', 'hashtag_text'])

        # update and insert tweet table
        tn = 'tweet'
        dfs[tn] = pd.merge(dfs[tn], df_user, on='user_raw_id')
        if not dfs[tn].empty and tn not in ignore_tables:
            stmt_do_nothing = insert(Tweet).returning(
                Tweet.__table__.c.raw_id).values(
                    dfs[tn][PMETA[tn]['d_keys']].to_dict(orient='record')
                ).on_conflict_do_nothing(index_elements=PMETA[tn]['du_keys'])
            rs = session.execute(stmt_do_nothing)
            duplicated_tweet_raw_ids = set(dfs[tn].raw_id) - set(r[0]
                                                                 for r in rs)
            if len(duplicated_tweet_raw_ids) != 0:
                logger.warning('Existed tweets with raw ids: %s',
                               duplicated_tweet_raw_ids)
            session.commit()

        # update and insert ass_url_platform
        # ass_url_platform is not in PMETA, it is constructed as:
        df_url_platform = df_url[['url_id']].copy()
        df_url_platform['platform_id'] = platform_id
        if not df_url_platform.empty and 'ass_url_platform' not in ignore_tables:
            stmt_do_nothing = insert(AssUrlPlatform).values(
                df_url_platform.to_dict(orient='record')
            ).on_conflict_do_nothing(index_elements=['url_id', 'platform_id'])
            session.execute(stmt_do_nothing)
            session.commit()

        # Fetch tweet table
        tn = 'tweet'
        if not dfs[tn].empty:
            q = """
            SELECT tw.id AS tweet_id, tw.raw_id AS tweet_raw_id
            FROM UNNEST(:tweet_raw_ids) AS t(tweet_raw_id)
                JOIN tweet AS tw ON tw.raw_id=t.tweet_raw_id
            """
            rs = session.execute(
                text(q).bindparams(tweet_raw_ids=dfs['tweet'].raw_id.tolist()))
            df_tweet = pd.DataFrame(iter(rs), columns=rs.keys())
        else:
            df_tweet = pd.DataFrame([], columns=['tweet_id', 'tweet_raw_id'])

        # update and insert ass_tweet_url
        tn = 'ass_tweet_url'
        dfs[tn] = pd.merge(dfs[tn], df_url, on='url_raw')
        dfs[tn] = pd.merge(dfs[tn], df_tweet, on='tweet_raw_id')
        if not dfs[tn].empty and tn not in ignore_tables:
            stmt_do_nothing = insert(AssTweetUrl).values(
                dfs[tn][PMETA[tn]['d_keys']].to_dict(orient='record')
            ).on_conflict_do_nothing(index_elements=PMETA[tn]['du_keys'])
            session.execute(stmt_do_nothing)
            session.commit()

        # update and insert ass_tweet
        tn = 'ass_tweet'
        dfs[tn] = pd.merge(
            dfs[tn], df_tweet,
            on='tweet_raw_id').rename(columns=dict(tweet_id='id'))
        if not dfs[tn].empty and tn not in ignore_tables:
            stmt_do_nothing = insert(AssTweet).values(
                dfs[tn][PMETA[tn]['d_keys']].to_dict(orient='record')
            ).on_conflict_do_nothing(index_elements=PMETA[tn]['du_keys'])
            session.execute(stmt_do_nothing)
            session.commit()

        # update and insert ass_tweet_hashtag
        tn = 'ass_tweet_hashtag'
        dfs[tn] = pd.merge(dfs[tn], df_hashtag, on='hashtag_text')
        dfs[tn] = pd.merge(dfs[tn], df_tweet, on='tweet_raw_id')
        if not dfs[tn].empty and tn not in ignore_tables:
            stmt_do_nothing = insert(AssTweetHashtag).values(
                dfs[tn][PMETA[tn]['d_keys']].to_dict(orient='record')
            ).on_conflict_do_nothing(index_elements=PMETA[tn]['du_keys'])
            session.execute(stmt_do_nothing)
            session.commit()

        # update and insert twitter_network_edge
        tn = 'twitter_network_edge'
        dfs[tn] = pd.merge(dfs[tn], df_url, on='url_raw')
        dfs[tn] = pd.merge(dfs[tn], df_tweet, on='tweet_raw_id')
        if not dfs[tn].empty and tn not in ignore_tables:
            stmt_do_nothing = insert(TwitterNetworkEdge).values(
                dfs[tn][PMETA[tn]['d_keys']].to_dict(orient='record')
            ).on_conflict_do_nothing(index_elements=PMETA[tn]['du_keys'])
            session.execute(stmt_do_nothing)
            session.commit()

    def bulk_parse_and_save(self,
                            jds,
                            session,
                            platform_id,
                            multiprocesses=False,
                            ignore_tables=[]):
        """A combined bulk operations: first parse a bucket of tweets, then save
        the parsed results into database.
        """
        if jds:
            parsed_results = self.parse_many(
                jds, multiprocesses=multiprocesses)
            if parsed_results:
                dfs = self.to_dict(parsed_results)
                self.bulk_save(session, dfs, platform_id, ignore_tables)
            else:
                logger.warning('No parsed results from these tweets!')
