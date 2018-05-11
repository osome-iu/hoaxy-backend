0  # -*- coding: utf-8 -*-
"""Implement parser to parse tweet received from twitter streaming.
"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

from hoaxy.database import Session
from hoaxy.database.functions import get_or_create_m
from hoaxy.database.functions import get_or_create_murl
from hoaxy.database.functions import create_or_update_muser
from hoaxy.database.functions import create_m
from hoaxy.database.models import AssTweetHashtag
from hoaxy.database.models import AssTweetUrl
from hoaxy.database.models import Hashtag
from hoaxy.database.models import Tweet
from hoaxy.database.models import TwitterUser
from hoaxy.database.models import TwitterUserUnion
from hoaxy.database.models import TwitterNetworkEdge
from hoaxy.database.models import AssTweet
from hoaxy.database.models import MAX_URL_LEN
from hoaxy.utils.dt import utc_from_str
from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert
import simplejson as json
import Queue
import logging
import threading
import time

logger = logging.getLogger(__name__)


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
        logger.warning(r'NULL byte (\u0000) found in %r and deleted!', jd['id'])
        if fp is not None:
            fp.write(jd['id_str'])
            fp.write('\n')
        data = data.replace(r'\\u0000', new)
        return json.loads(data, encoding='utf-8')
    else:
        return jd


class Parser():
    """This class parses a tweet and save all related parsed relationships
    into the database, see the main function `parse`.
    """

    def __init__(self,
                 session,
                 platform_id,
                 save_none_url_tweet=True,
                 saved_tweet=False,
                 file_save_null_byte_tweet=None):
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
        self.saved_tweet = saved_tweet
        if file_save_null_byte_tweet is not None:
            self.fp = open(file_save_null_byte_tweet, 'w')
        else:
            self.fp = None

    def _parse_entities(self,
                        entities,
                        urls_set=None,
                        hashtags_set=None,
                        mentions_set=None):
        """Internal function to briefly parse the entities in a tweet object.

        By default postgresql column is case-sensitive and URL itself may
        be case sensitive, no need to convert case. However, for hashtags,
        twitter use them as case-insensitive, so we need to convert them
        into lower case.

        Parameters
        ----------
        entities: JSON object
            The entities field of a tweet object.
        urls_set: set
            A set to store all urls in one parsing process.
        hashtags_set: set
            A set to store all hashtags in one parsing process.
        mentions_set: set
            A set to store all mentions in one parsing process.
        """
        if urls_set is not None and 'urls' in entities:
            for url in entities['urls']:
                u = url.get('expanded_url')
                if u:
                    urls_set.add(u)
        # hashtag in twitter is case insensitive, shall convert
        if hashtags_set is not None and 'hashtags' in entities:
            for h in entities['hashtags']:
                htext = h.get('text')
                if htext:
                    hashtags_set.add(htext.lower())
        if mentions_set is not None and 'user_mentions' in entities:
            for m in entities['user_mentions']:
                user_raw_id = m.get('id')
                screen_name = m.get('screen_name')
                if user_raw_id and screen_name:
                    mentions_set.add((user_raw_id, screen_name))

    def _save_edges(self, url_map, entities, tweet_id, tweet_raw_id,
                    from_raw_id, to_raw_id, is_quoted_url, is_mention,
                    tweet_type):
        """Parsing and saving URLs in one entities, and building and saving
        the network edge.d
        """
        if 'urls' in entities:
            for url in entities['urls']:
                u = url.get('expanded_url')
                if u:
                    # saving edges
                    self.session.add(
                        TwitterNetworkEdge(
                            tweet_raw_id=tweet_raw_id,
                            from_raw_id=from_raw_id,
                            to_raw_id=to_raw_id,
                            url_id=url_map[u],
                            is_quoted_url=is_quoted_url,
                            is_mention=is_mention,
                            tweet_type=tweet_type))
                    try:
                        self.session.commit()
                    except IntegrityError as e:
                        logger.error('twitter_network_edge IntegrityError: %s',
                                     e)
                        self.session.rollback()

    def parse(self, jd):
        """The main parse function.

        Parameters
        ---------
        jd : json
            Tweet json data.

        Procedures
        ----------
        1) do roughly parsing to validate `jd`
        2) carefully parsing and insert into database
        3) other associations
        """
        logger.debug('Parsing one tweet, begin ...')
        #
        # 1) do roughly parsing to validate the tweet
        #
        # 1-1) parsing necessary fields, if failed then it is not a valid tweet
        logger.debug('Replacing null byte if existing ...')
        jd = replace_null_byte(jd, self.fp)
        logger.debug('1) Roughly parsing ...')
        try:
            tw_raw_id = jd['id']
            created_at = utc_from_str(jd['created_at'])
            user_raw_id = jd['user']['id']
        except KeyError as e:
            logger.error('Invalid tweet: %s', e)
            return None
        # 1-2) roughly parsing
        entities_list = []
        quoted_status_id = None
        retweeted_status_id = None
        if 'entities' in jd:
            entities_list.append(jd['entities'])
        if 'quoted_status' in jd:
            quoted_jd = jd['quoted_status']
            quoted_user_jd = jd['quoted_status']['user']
            quoted_status_id = quoted_jd['id']
            if 'entities' in quoted_jd:
                entities_list.append(quoted_jd['entities'])
        if 'retweeted_status' in jd:
            retweeted_jd = jd['retweeted_status']
            retweeted_user_jd = jd['retweeted_status']['user']
            retweeted_status_id = retweeted_jd['id']
            if 'entities' in retweeted_jd:
                entities_list.append(retweeted_jd['entities'])
        in_reply_to_status_id = jd['in_reply_to_status_id']
        in_reply_to_user_id = jd['in_reply_to_user_id']
        in_reply_to_screen_name = jd['in_reply_to_screen_name']

        urls_set = set()
        hashtags_set = set()
        mentions_set = set()
        for entities in entities_list:
            if entities:
                self._parse_entities(entities, urls_set, hashtags_set,
                                     mentions_set)
        # This tweet should contain urls
        if len(urls_set) == 0 and self.save_none_url_tweet is False:
            logger.warning('No url found in tweet %s, ignore!', tw_raw_id)
            return None
        #
        # 2) carefully parsing and saving into database
        #
        logger.debug('2) Carefully parsing and saving ...')
        logger.debug('2-0) Saving twitter_user raw_id=%s ...', user_raw_id)
        muser = get_or_create_m(
            self.session,
            TwitterUser,
            data=dict(raw_id=user_raw_id),
            fb_uk='raw_id')
        logger.debug('Saving this user into twitter_user_union as well ...')
        create_or_update_muser(
            self.session,
            data=dict(
                raw_id=user_raw_id,
                screen_name=jd['user']['screen_name'],
                followers_count=jd['user']['followers_count'],
                profile=jd['user'],
                updated_at=created_at))
        # creating tweet
        logger.debug('2-0) Saving tweet raw_id=%s ...', tw_raw_id)
        if self.saved_tweet is True:
            mtweet = self.session.query(Tweet).filter_by(raw_id=tw_raw_id).one()
        else:
            mtweet = Tweet(
                raw_id=tw_raw_id,
                json_data=jd,
                created_at=created_at,
                user_id=muser.id)
            self.session.add(mtweet)
            try:
                self.session.commit()
                logger.debug('Inserted tweet %r', tw_raw_id)
            except IntegrityError as e:
                logger.warning('Tweet %s existed in db: %s', tw_raw_id, e)
                self.session.rollback()
                return None
        tweet_id = mtweet.id
        # Saving all urls and mapping the saved id
        url_map = dict()
        logger.debug('2-0) Saving all urls and associating with tweet...')
        for url in urls_set:
            murl = get_or_create_murl(
                self.session, data=dict(raw=url), platform_id=self.platform_id)
            url_map[url] = murl.id
            # saving ass_tweet_url
            if self.saved_tweet is False:
                self.session.add(
                    AssTweetUrl(tweet_id=tweet_id, url_id=url_map[url]))
                try:
                    self.session.commit()
                except IntegrityError as e:
                    logger.error('ass_tweet_url IntegrityError, see: %s', e)
                    self.session.rollback()
        # 2-1) retweet, focusing on retweeted_status
        #               edge direction: from retweeted_user to current user
        if retweeted_status_id is not None:
            logger.debug(
                '2-1-a) Saving the retweeted user into twitter_user_union ...')
            retweeted_user_id = retweeted_user_jd['id']
            retweeted_screen_name = retweeted_user_jd['screen_name']
            create_or_update_muser(
                self.session,
                data=dict(
                    raw_id=retweeted_user_id,
                    screen_name=retweeted_screen_name,
                    followers_count=retweeted_user_jd['followers_count'],
                    profile=retweeted_user_jd,
                    updated_at=created_at))
            # retweeted user has been saved above, should be removed from mentions
            try:
                mentions_set.remove((retweeted_user_id, retweeted_screen_name))
            except KeyError as e:
                logger.warning('Tweet %r: retweeted user not in mentions',
                               tw_raw_id)
            logger.debug('2-1-a) Saving edges for retweet ...')
            self._save_edges(
                url_map,
                retweeted_jd['entities'],
                tweet_id,
                tw_raw_id,
                from_raw_id=retweeted_user_id,
                to_raw_id=user_raw_id,
                is_quoted_url=False,
                is_mention=False,
                tweet_type='retweet')
        # 2-2) reply, focusing on current status
        #             edges direction: from current user to mentions
        if in_reply_to_status_id is not None:
            # mentioned users would be saved later
            logger.debug('2-1-b) Saving edges for reply ...')
            # in_reply_to_user
            self._save_edges(
                url_map,
                jd['entities'],
                tweet_id,
                tw_raw_id,
                from_raw_id=user_raw_id,
                to_raw_id=in_reply_to_user_id,
                is_quoted_url=False,
                is_mention=False,
                tweet_type='reply')
            # mentions
            for m in jd['entities']['user_mentions']:
                to_raw_id = m.get('id')
                if to_raw_id and to_raw_id != in_reply_to_user_id:
                    self._save_edges(
                        url_map,
                        jd['entities'],
                        tweet_id,
                        tw_raw_id,
                        from_raw_id=user_raw_id,
                        to_raw_id=to_raw_id,
                        is_quoted_url=False,
                        is_mention=True,
                        tweet_type='reply')
        # 2-3) quote
        if quoted_status_id is not None:
            logger.debug(
                '2-1-c) Saving the quoted user into twitter_user_union ...')
            quoted_user_id = quoted_user_jd['id']
            quoted_screen_name = quoted_user_jd['screen_name']
            create_or_update_muser(
                self.session,
                data=dict(
                    raw_id=quoted_user_id,
                    screen_name=quoted_screen_name,
                    followers_count=quoted_user_jd['followers_count'],
                    profile=quoted_user_jd,
                    updated_at=created_at))
            # 2-3-1) retweeted quote, focusing on quoted_status
            #                         treated as retweet edge
            if retweeted_status_id is not None:
                logger.debug(
                    '2-1-c) Saving edges for quoting part of retweet ...')
                self._save_edges(
                    url_map,
                    quoted_jd['entities'],
                    tweet_id,
                    tw_raw_id,
                    from_raw_id=retweeted_user_jd['id'],
                    to_raw_id=user_raw_id,
                    is_quoted_url=True,
                    is_mention=False,
                    tweet_type='retweet')
            # 2-3-2) replied quote, focusing on quoted_status
            #                       treated as reply edge
            elif in_reply_to_status_id is not None:
                logger.debug(
                    '2-1-c) Saving edges for quoting part of reply ...')
                # in_reply_to_user
                self._save_edges(
                    url_map,
                    quoted_jd['entities'],
                    tweet_id,
                    tw_raw_id,
                    from_raw_id=user_raw_id,
                    to_raw_id=in_reply_to_user_id,
                    is_quoted_url=True,
                    is_mention=False,
                    tweet_type='reply')
                # mentions
                for m in jd['entities']['user_mentions']:
                    to_raw_id = m.get('id')
                    if to_raw_id and to_raw_id != in_reply_to_user_id:
                        self._save_edges(
                            url_map,
                            quoted_jd['entities'],
                            tweet_id,
                            tw_raw_id,
                            from_raw_id=user_raw_id,
                            to_raw_id=to_raw_id,
                            is_quoted_url=True,
                            is_mention=True,
                            tweet_type='reply')
            # 2-3-3) pure quote
            else:
                logger.debug(
                    '2-1-c) Saving edge for pure quote part of quote ...')
                self._save_edges(
                    url_map,
                    quoted_jd['entities'],
                    tweet_id,
                    tw_raw_id,
                    from_raw_id=quoted_user_jd['id'],
                    to_raw_id=user_raw_id,
                    is_quoted_url=True,
                    is_mention=False,
                    tweet_type='quote')
                logger.debug(
                    '2-1-c) Saving edges for original part of quote ...')
                for m in jd['entities']['user_mentions']:
                    to_raw_id = m.get('id')
                    if to_raw_id:
                        self._save_edges(
                            url_map,
                            jd['entities'],
                            tweet_id,
                            tw_raw_id,
                            from_raw_id=user_raw_id,
                            to_raw_id=to_raw_id,
                            is_quoted_url=False,
                            is_mention=True,
                            tweet_type='quote')
        # 2-4) original tweet
        if retweeted_status_id is None and in_reply_to_status_id is None\
            and quoted_status_id is None and 'entities' in jd and\
            'user_mentions' in jd['entities']:
            logger.debug('2-1-d) Saving edges for original tweet ...')
            for m in jd['entities']['user_mentions']:
                to_raw_id = m.get('id')
                if to_raw_id:
                    self._save_edges(
                        url_map,
                        jd['entities'],
                        tweet_id,
                        tw_raw_id,
                        from_raw_id=user_raw_id,
                        to_raw_id=to_raw_id,
                        is_quoted_url=False,
                        is_mention=True,
                        tweet_type='origin')
        # saving all mentions ...
        logger.debug('3) Saving all mentions ...')
        # add the in_reply_to_user
        mentions_set.add((in_reply_to_user_id, in_reply_to_screen_name))
        for user_raw_id, screen_name in mentions_set:
            create_or_update_muser(
                self.session,
                data=dict(
                    raw_id=user_raw_id,
                    screen_name=screen_name,
                    updated_at=created_at))
        # saving hashtags
        logger.debug('3) creating hashtags')
        if self.saved_tweet is False:
            for hashtag in hashtags_set:
                mhashtag = get_or_create_m(
                    self.session,
                    Hashtag,
                    data=dict(text=hashtag),
                    fb_uk='text')
                self.session.add(
                    AssTweetHashtag(tweet_id=tweet_id, hashtag_id=mhashtag.id))
                try:
                    self.session.commit()
                except IntegrityError as e:
                    logger.error('ass_tweet_hashtag IntegrityError, see: %s', e)
                    self.session.rollback()
        # saving associate tweet
        logger.debug('3 Saving ass_tweet ...')
        if self.saved_tweet is False:
            create_m(
                self.session,
                AssTweet,
                data=dict(
                    id=tweet_id,
                    retweeted_status_id=retweeted_status_id,
                    quoted_status_id=quoted_status_id,
                    in_reply_to_status_id=in_reply_to_status_id))
        logger.debug('Parsing one tweet, done.')


class BulkParser():
    """Parse tweet object into separated buckets that store all associated tables.
       And provide a function to save these buckets into tables.
    """

    def __init__(self, platform_id=None, save_none_url_tweet=True):
        """Constructor of Parser.

        Parameters
        ----------
        platform_id : int
            The id of platform (should be twitter platform object).
        save_none_url_tweet : bool
            Whether save the tweet if no URLs in the tweet.
        """
        self.platform_id = platform_id
        self.save_none_url_tweet = save_none_url_tweet

    def _parse_entities(self,
                        entities,
                        label,
                        l_urls=None,
                        l_mentions=None,
                        l_hashtags=None):
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
            The label of the entities, e.g., 'this' for current tweet, 'retweet' for the
            retweeted_status of current tweet.
        l_urls: dict of set
            A dict to store all urls in one parsing process.
        l_mentions: dict
            A dict to store all mentions in one parsing process.
        l_hashtags: dict
            A dict to store all hashtags in one parsing process.
        """
        if l_urls is not None and 'urls' in entities:
            for url in entities['urls']:
                u = url.get('expanded_url')
                if u:
                    l_urls[label].add(u)
                    l_urls['union'].add(u)
        if l_mentions is not None and 'user_mentions' in entities:
            for m in entities['user_mentions']:
                user_raw_id = m.get('id')
                screen_name = m.get('screen_name')
                if user_raw_id and screen_name:
                    l_mentions[label].add((user_raw_id, screen_name))
                    l_mentions['union'].add((user_raw_id, screen_name))
        # hashtag in twitter is case insensitive, shall be converted
        if l_hashtags is not None and 'hashtags' in entities:
            for h in entities['hashtags']:
                htext = h.get('text')
                if htext:
                    htext = htext.lower()
                    l_hashtags[label].add(htext)
                    l_hashtags['union'].add(htext)

    def _parse_l1(self, jd):
        """First level parsing, which collect and parse all associated entities.

        Parameters
        ----------
        jd: JSON
            A tweet JSON object.

        Returns
        ------
        Tuple (l_urls, l_mentions, l_hashtags) representing associated URLs,
        user_mentions and hashtags.
        """
        # this status
        l_urls = dict(this=set(), quote=set(), retweet=set(), union=set())
        l_mentions = dict(this=set(), quote=set(), retweet=set(), union=set())
        l_hashtags = dict(this=set(), quote=set(), retweet=set(), union=set())
        if 'entities' in jd:
            self._parse_entities(jd['entities'], 'this', l_urls, l_mentions,
                                 l_hashtags)
        if 'retweeted_status' in jd and 'entities' in jd['retweeted_status']:
            self._parse_entities(jd['retweeted_status']['entities'], 'retweet',
                                 l_urls, l_mentions, l_hashtags)
        if 'quoted_status' in jd and 'entities' in jd['quoted_status']:
            self._parse_entities(jd['quoted_status']['entities'], 'quote',
                                 l_urls, l_mentions, l_hashtags)
        return l_urls, l_mentions, l_hashtags

    def _parse_l2(self, jd, l_urls, l_mentions, g_urls_map, g_uusers_set,
                  g_edges_set):
        """Second Level parsing, to build users_union set and edges set.

        This function should be called only after tweet, twitter_user,
        urls, ass_tweet, ass_tweet_url are saved.

        Parameters
        ---------
        jd: JSON
            A tweet JSON object.
        l_urls: dict
            A dict storing all associated urls of this tweet.
        l_mentions: dict
            A dict storing all associated mentions of this tweet.
        g_urls_map: dict
            A dict map, where key is URL string and value is
            the database index id of this URL. Keys should include
            all URLs in `l_urls`. If url_id is set to -1 for the
            unavailable URLs.
        g_uusers_set: set
            A set to store users_union. New user_unions should be
            added into this set.
        g_edges_set: set
            A set to store edges. New edges should be added into
            this set.
        """
        tweet_raw_id = jd['id']
        user_raw_id = jd['user']['id']
        user_screen_name = jd['user']['screen_name']
        quoted_status_id = None
        retweeted_status_id = None
        if 'quoted_status' in jd:
            quoted_user_id = jd['quoted_status']['user']['id']
            quoted_screen_name = jd['quoted_status']['user']['screen_name']
            quoted_status_id = jd['quoted_status']['id']
        if 'retweeted_status' in jd:
            retweeted_user_id = jd['retweeted_status']['user']['id']
            retweeted_screen_name = jd['retweeted_status']['user'][
                'screen_name']
            retweeted_status_id = jd['retweeted_status']['id']
        in_reply_to_status_id = jd['in_reply_to_status_id']
        in_reply_to_user_id = jd['in_reply_to_user_id']
        in_reply_to_screen_name = jd['in_reply_to_screen_name']
        logger.debug('Level 2 parsing, building users and edges set...')
        logger.debug('Adding current_user into twitter_user_union  ...')
        g_uusers_set.add((user_raw_id, user_screen_name))
        # 2-1) retweet, focusing on retweeted_status
        #               edge direction: from retweeted_user to current user
        if retweeted_status_id is not None:
            logger.debug(
                '2-1-a) Adding retweeted_user into twitter_user_union ...')
            g_uusers_set.add((retweeted_user_id, retweeted_screen_name))
            logger.debug('2-1-a) building edges for retweet ...')
            for u in l_urls['retweet']:
                g_edges_set.add((tweet_raw_id, retweeted_user_id, user_raw_id,
                                 g_urls_map[u], False, False, 'retweet'))
        # 2-2) reply, focusing on current status
        #             edges direction: from current user to mentions
        if in_reply_to_status_id is not None:
            logger.debug(
                '2-1-b) Adding in_reply_to_user into twitter_user_union ...')
            g_uusers_set.add((in_reply_to_user_id, in_reply_to_screen_name))
            logger.debug('2-1-b) building edges for reply ...')
            # in_reply_to_user, edge
            for u in l_urls['this']:
                g_edges_set.add((tweet_raw_id, user_raw_id, in_reply_to_user_id,
                                 g_urls_map[u], False, False, 'reply'))
            # mentions, edges
            for mention_id, mention_screen_name in l_mentions['this']:
                if mention_id != in_reply_to_user_id:
                    for u in l_urls['this']:
                        g_edges_set.add((tweet_raw_id, user_raw_id, mention_id,
                                         g_urls_map[u], False, True, 'reply'))
        # 2-3) quote
        if quoted_status_id is not None:
            logger.debug(
                '2-1-c) Adding quoted_user into twitter_user_union ...')
            g_uusers_set.add((quoted_user_id, quoted_screen_name))
            # 2-3-1) retweeted quote, focusing on quoted_status
            #                         treated as retweet edge
            if retweeted_status_id is not None:
                logger.debug(
                    '2-1-c) building edges for the quoting part of a retweet ...'
                )
                for u in l_urls['quote']:
                    g_edges_set.add(
                        (tweet_raw_id, retweeted_user_id, user_raw_id,
                         g_urls_map[u], True, False, 'retweet'))
            # 2-3-2) replied quote, focusing on quoted_status
            #                       treated as reply edge
            elif in_reply_to_status_id is not None:
                logger.debug(
                    '2-1-c) building edges for the quoting part of a reply ...')
                # in_reply_to_user, edges for quoted url
                for u in l_urls['quote']:
                    g_edges_set.add(
                        (tweet_raw_id, user_raw_id, in_reply_to_user_id,
                         g_urls_map[u], True, False, 'reply'))
                # mentions, edges for quoted url
                for mention_id, mention_screen_name in l_mentions['this']:
                    if mention_id != in_reply_to_user_id:
                        for u in l_urls['quote']:
                            g_edges_set.add(
                                (tweet_raw_id, user_raw_id, mention_id,
                                 g_urls_map[u], True, True, 'reply'))
            # 2-3-3) pure quote
            else:
                logger.debug(
                    '2-1-c) Building edges for quote part of the pure quote ...'
                )
                for u in l_urls['quote']:
                    g_edges_set.add((tweet_raw_id, quoted_user_id, user_raw_id,
                                     g_urls_map[u], True, False, 'quote'))
                logger.debug(
                    '2-1-c) building edges for original part of the pure quote ...'
                )
                for mention_id, mention_screen_name in l_mentions['this']:
                    for u in l_urls['this']:
                        g_edges_set.add((tweet_raw_id, user_raw_id, mention_id,
                                         g_urls_map[u], False, True, 'quote'))
                    for u in l_urls['quote']:
                        g_edges_set.add((tweet_raw_id, user_raw_id, mention_id,
                                         g_urls_map[u], True, True, 'quote'))
        # 2-4) original tweet
        if retweeted_status_id is None and in_reply_to_status_id is None\
                and quoted_status_id is None:
            logger.debug('2-1-d) building edges for original tweet ...')
            for mention_id, mention_screen in l_mentions['this']:
                for u in l_urls['this']:
                    g_edges_set.add((tweet_raw_id, user_raw_id, mention_id,
                                     g_urls_map[u], False, True, 'origin'))
        # saving all mentions ...
        logger.debug('Adding all mentions into twitter_user_union...')
        # import pdb; pdb.set_trace()
        for m in l_mentions['union']:
            g_uusers_set.add(m)

    def parse_existed_one(self, tw_id, jd, session, g_urls_map, g_uusers_set,
                          g_edges_set):
        """The main parse function. This function will parse tweet into different
        components corresponding to related table records.

        Parameters
        ---------
        jd : json
            Tweet json data.
        tw_id : integer
            If tweet has been saved, tw_db_id is the id of
        """
        logger.debug('Parsing tweet %r begin ...', jd['id'])
        logger.debug('Level 1 parsing, roughly parse ...')
        l_urls, l_mentions, l_hashtags = self._parse_l1(jd)
        # Make sure we do saved and fetched all url_ids
        for u in l_urls['union']:
            if g_urls_map.get(u) is None:
                if len(u) > MAX_URL_LEN:
                    logger.warning(
                        'URL %s of tweet %s was ignored because of too long', u,
                        jd['id'])
                    murl_id = -1
                else:
                    logger.warning(
                        'Previously incomplete parsing, missing %s of tweet %s',
                        u, jd['id'])
                    murl_id = get_or_create_murl(
                        session, data=dict(raw=u),
                        platform_id=self.platform_id).id
                    # Saving AssTweetUrl
                    session.add(AssTweetUrl(tweet_id=tw_id, url_id=murl_id))
                    try:
                        session.commit()
                    except IntegrityError as e:
                        logger.error('ass_tweet_url IntegrityError, see: %s', e)
                        session.rollback()
                g_urls_map[u] = murl_id
        logger.debug('Level 2 parsing, deeply parse ...')
        self._parse_l2(jd, l_urls, l_mentions, g_urls_map, g_uusers_set,
                       g_edges_set)

    def parse_new_one(self, jd, session, g_urls_map, g_uusers_set, g_edges_set):
        # validate jd
        jd = replace_null_byte(jd)
        try:
            tw_raw_id = jd['id']
            created_at = utc_from_str(jd['created_at'])
            user_raw_id = jd['user']['id']
        except KeyError as e:
            logger.error('Invalid tweet: %s', e)
            return None
        # parsing, level 1
        l_urls, l_mentions, l_hashtags = self._parse_l1(jd)
        if len(l_urls['union']) == 0 and self.save_none_url_tweet is False:
            logger.warning('Ignore tweet %r with no urls!', tw_raw_id)
            return None
        # saving, level 1
        logger.debug('Saving this user ...')
        muser = get_or_create_m(
            session, TwitterUser, data=dict(raw_id=user_raw_id), fb_uk='raw_id')
        logger.debug('Saving this tweet ...')
        muser_id = muser.id
        mtweet = Tweet(
            raw_id=tw_raw_id,
            json_data=jd,
            created_at=created_at,
            user_id=muser_id)
        session.add(mtweet)
        try:
            session.commit()
            logger.debug('Inserted tweet %r', tw_raw_id)
        except IntegrityError as e:
            logger.warning('Tweet %s existed in db: %s', tw_raw_id, e)
            session.rollback()
            return None
        mtweet_id = mtweet.id
        logger.debug('Saving AssTweet ...')
        retweeted_status_id = None
        quoted_status_id = None
        if 'quoted_status' in jd:
            quoted_status_id = jd['quoted_status']['id']
        if 'retweeted_status' in jd:
            retweeted_status_id = jd['retweeted_status']['id']
        in_reply_to_status_id = jd['in_reply_to_status_id']
        session.add(
            AssTweet(
                retweeted_status_id=retweeted_status_id,
                quoted_status_id=quoted_status_id,
                in_reply_to_status_id=in_reply_to_status_id))
        try:
            session.commit()
        except IntegrityError as e:
            logger.warning(e)
            session.rollback()
        logger.debug('Saving urls ...')
        for u in l_urls['union']:
            if len(u) > MAX_URL_LEN:
                murl_id = -1
            else:
                murl_id = get_or_create_murl(
                    session, data=dict(raw=u), platform_id=self.platform_id).id
                # Saving AssTweetUrl
                session.add(AssTweetUrl(tweet_id=mtweet_id, url_id=murl_id))
                try:
                    session.commit()
                except IntegrityError as e:
                    logger.error('ass_tweet_url IntegrityError, see: %s', e)
                    session.rollback()
            g_urls_map[u] = murl_id
        # creating hashtags
        logger.debug('creating hashtags ...')
        for hashtag in l_hashtags['union']:
            mhashtag = get_or_create_m(
                session, Hashtag, data=dict(text=hashtag), fb_uk='text')
            session.add(
                AssTweetHashtag(tweet_id=mtweet.id, hashtag_id=mhashtag.id))
            try:
                session.commit()
            except IntegrityError as e:
                logger.error('ass_tweet_hashtag IntegrityError, see: %s', e)
                session.rollback()
        self._parse_l2(jd, l_urls, l_mentions, g_urls_map, g_uusers_set,
                       g_edges_set)

    def save_bulk(self, session, g_uusers_set, g_edges_set):
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
        if len(edges) > 0:
            session.bulk_insert_mappings(TwitterNetworkEdge, edges)
            session.commit()
        if len(uusers) > 0:
            stmt_do_nothing = insert(TwitterUserUnion).values(
                uusers).on_conflict_do_nothing(index_elements=['raw_id'])
            session.execute(stmt_do_nothing)
            session.commit()

    def bulk_parse_and_save(self,
                            session,
                            jds,
                            existed_tweets=False,
                            g_urls_map=None,
                            urls_cache_size=1000):
        g_uusers_set = set()
        g_edges_set = set()
        if existed_tweets is False:
            g_urls_map = dict()
            for jd in jds:
                self.parse_new_one(
                    jd,
                    session=session,
                    g_urls_map=g_urls_map,
                    g_uusers_set=g_uusers_set,
                    g_edges_set=g_edges_set)
        else:
            if isinstance(jds, dict):
                for tw_id, jd in jds.iteritems():
                    self.parse_existed_one(
                        tw_id,
                        jd,
                        session,
                        g_urls_map=g_urls_map,
                        g_uusers_set=g_uusers_set,
                        g_edges_set=g_edges_set)
            else:
                raise TypeError('Input jds should be dict!')
        self.save_bulk(session, g_uusers_set, g_edges_set)


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

    def stop(self):
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
        parser = BulkParser(platform_id=self.platform_id, **self.p_kwargs)
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
                g_urls_map = dict()
                g_uusers_set = set()
                g_edges_set = set()
                parser.parse_new_one(
                    jd,
                    session,
                    g_urls_map=g_urls_map,
                    g_uusers_set=g_uusers_set,
                    g_edges_set=g_edges_set)
                parser.save_bulk(session, g_uusers_set, g_edges_set)

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
