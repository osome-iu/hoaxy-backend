# -*- coding: utf-8 -*-
"""A SQLAlchemy style database models.

Intended for database server PostgreSQL >= 9.3, as we use json datatype.

You can modify them for your database dialect.
"""

#
# written by Chengcheng Shao <sccotte@gmail.com>

from datetime import datetime
from sqlalchemy import Column, ForeignKey, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import deferred
from sqlalchemy.orm import relationship
from sqlalchemy.schema import UniqueConstraint, Index
from sqlalchemy.types import SmallInteger, Integer, BigInteger, DateTime
from sqlalchemy.types import String, Enum, Boolean, Text, Date
import dateutil.parser
import logging
import re

# name of claim
N_CLAIM = 'claim'
# name of fact checking
N_FACT_CHECKING = 'fact_checking'
# name of twitter platform
N_PLATFORM_TWITTER = 'twitter'
# name of web platform
N_PLATFORM_WEB = 'news_website'
# enum type for platform
PLATFORM_TYPES = ('social_network', 'news_website')
# type of social and web
T_PLATFORM_SOCIAL, T_PLATFORM_WEB = PLATFORM_TYPES
# twitter platform data
TWITTER_PLATFORM_DICT = dict(
    name=N_PLATFORM_TWITTER, platform_type=T_PLATFORM_SOCIAL)
# web platform data
WEB_PLATFORM_DICT = dict(name=N_PLATFORM_WEB, platform_type=T_PLATFORM_WEB)
# The maximum length of URL allowed.
MAX_URL_LEN = 2083

Base = declarative_base()
logger = logging.getLogger(__name__)

# URL STATUS CODE
U_DEFAULT = 0
# URL, fetch html,  success
U_HTML_SUCCESS = 40
# URL, fetch html, the domain of the url is  in excluded domains
U_HTML_ERROR_EXCLUDED_DOMAIN = 41
# URL, fetch html, url is home URL, not an article URL
U_HTML_ERROR_HOME_URL = 42
# URL, fetch html, cannot canonicalize or urlparse expanded url
U_HTML_ERROR_INVALID_URL = 43
# URL, fetch html, When sending request, non-http error happens,
# e.g., domain resolve failed
U_HTML_ERROR_NONHTTP = 44
# URL, fetch html, returned reponse is not html document, e.g., image
U_HTML_ERROR_INVALID_HTML = 45
# URL, when closing the spider and the reason is finished
# then set all sended url with unchanged status_code to this value
# indicate that we never recevied their scrapy response
U_HTML_ERROR_DROPPED = 46
# unknown error
U_HTML_ERROR_UNKNOWN = 49

# default parsing status
A_DEFAULT = 0
# article parsed, success
A_P_SUCCESS = 80
# Invalided data returned from web parser
A_WP_ERROR_DATA_INVALID = 81
# URL, fetch html, When sending request, non-http error happens,
# e.g., domain resolve failed
A_WP_ERROR_NONHTTP = 44
# WHEN CLOSING THE SPIDER WITH reason='finished', THOSE NO RETURNED REQUEST
# WILL BE MARKED WITH
A_WP_ERROR_DROPPED = 86
# WHEN PARSING ARTICLE, UNKONW ERROR
A_WP_ERROR_UNKNOWN = 89

# SQL expressions
DEFAULT_WHERE_EXPR_FETCH_URL = """site.is_enabled is TRUE AND \
site.is_alive is TRUE"""
DEFAULT_WHERE_EXPR_FETCH_HTML = 'url.status_code={}'.format(U_DEFAULT)
DEFAULT_WHERE_EXPR_PARSE_ARTICLE = "status_code={}".format(A_DEFAULT)


class TableMixin(object):
    """A Mixin Class for all tables that inherent this class.

    The Mixin class will
    (1) Set the table name as the underscored format of class name, where
        the class name itself is in CamelCase.
    (2) Add `id` column as the primary key.
    """

    # table name is equal to class name (from CamelCase to underscored)
    @declared_attr
    def __tablename__(cls):
        tname = re.sub('(?!^)([A-Z]+)', r'_\1', cls.__name__).lower()
        logger.debug("Building schema of table %s", tname)
        return tname

    # id, keep `id` as the first column when create a table
    id = Column(Integer, primary_key=True)


class AuditColumns(object):
    """Another Mixin class that adds audit columns: created_at and updated_at.
    """
    created_at = Column(
        DateTime,
        default=datetime.utcnow,
        server_default=text("(now() at time zone 'utc')"))
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AssTweetUrl(TableMixin, Base):
    """Association table to connect table `tweet` and `url`."""
    tweet_id = Column(
        Integer, ForeignKey(
            'tweet.id', ondelete='CASCADE', onupdate='CASCADE'))
    url_id = Column(
        Integer, ForeignKey('url.id', ondelete='CASCADE', onupdate='CASCADE'))
    __table_args__ = (UniqueConstraint(
        'tweet_id', 'url_id', name='tweet_url_uq'),
                      Index('url_tweet_idx', 'url_id', 'tweet_id'))


class AssTweetHashtag(TableMixin, Base):
    """Association table to connect table `tweet` and `hashtag`."""
    tweet_id = Column(
        Integer, ForeignKey(
            'tweet.id', ondelete='CASCADE', onupdate='CASCADE'))
    hashtag_id = Column(
        Integer,
        ForeignKey('hashtag.id', ondelete='CASCADE', onupdate='CASCADE'))
    __table_args__ = (UniqueConstraint(
        'tweet_id', 'hashtag_id', name='tweet_hashtag_uq'), )


class AssUrlPlatform(TableMixin, Base):
    """Association table to connect table `url` and `platform`."""
    url_id = Column(
        Integer, ForeignKey('url.id', ondelete='CASCADE', onupdate='CASCADE'))
    platform_id = Column(
        Integer,
        ForeignKey('platform.id', ondelete='CASCADE', onupdate='CASCADE'))
    __table_args__ = (UniqueConstraint(
        'url_id', 'platform_id', name='url_platform_uq'), )


class AssSiteSiteTag(TableMixin, Base):
    """Association table to connect table `site` and `site_tag`."""
    site_id = Column(
        Integer, ForeignKey('site.id', ondelete='CASCADE', onupdate='CASCADE'))
    site_tag_id = Column(
        Integer,
        ForeignKey('site_tag.id', ondelete='CASCADE', onupdate='CASCADE'))
    __table_args__ = (UniqueConstraint(
        'site_id', 'site_tag_id', name='site_site_tag_id'), )


class Platform(TableMixin, AuditColumns, Base):
    """The platform table shows from which platform we catch the URL.

    Relations
    ---------
    platform <-- MANY TO MANY --> url
    """
    name = Column(String(31), unique=True, nullable=False)
    platform_type = Column(
        Enum(*PLATFORM_TYPES, name='platform_types'), nullable=False)
    # Relationship attributes
    urls = relationship(
        'Url',
        passive_deletes=True,
        secondary='ass_url_platform',
        back_populates='platforms')


class Url(TableMixin, AuditColumns, Base):
    """The url table store URLs collected either from social networks or
    websites.

    Relations
    ---------
    url <-- MANY TO ONE  --> site
    url <-- MANY TO ONE  --> artilce
    url <-- MANY TO MANY --> tweet
    url <-- MANY TO MANY --> platform

    Columns
    -------
    raw : string(MAX_URL_LEN)
        The original url from site or tweet.
    expended : string(MAX_URL_LEN)
        The unshorten version of url that uses shorten service.
    canonical : string(MAX_URL_LEN)
        The canonization of the raw_url.
    html : text
        HTML content of this url.
    status_code : integer
        The status of this record.
    ---------------------------------------------------------------------------
    CONSTANT                 | VALUE |     DESCRIPTION
    ---------------------------------------------------------------------------
    U_DEFAULT                | 0     | Default, when inserting by URL spiders
    U_HTML_SUCCESS           | 40    | Fetch HTML successfully
    U_HTML_ERROR_EXCLUDED
    _DOMAIN                  | 41    | Fetch HTML failed, excluded domain
    U_HTML_ERROR_HOME_URL    | 42    | Fetch HTML failed, home URL
    U_HTML_ERROR_INVALID_URL | 43    | Fetch HTML failed, invalid URL form
                                       e.g., urlparse failed
    U_HTML_ERROR_NONHTTP     | 44    | Fetch HTML failed, non-http error
                                       e.g., domain resolve failed
    U_HTML_ERROR_INVALID_HTML| 45    | Invalid HTML, e.g., image
    U_HTML_ERROR_DROPPED     | 46    | No scrapy response received
    null                     | >200  | Fetch HTML failed, HTTP code
    ################### HTML COLUMN PLACED TO ARTICLE TABLE ################
    ---------------------------------------------------------------------------
    """
    raw = Column(String(MAX_URL_LEN), unique=True, nullable=False)
    expanded = Column(String(MAX_URL_LEN))
    aliased = Column(String(MAX_URL_LEN))
    canonical = Column(String(MAX_URL_LEN))
    date_published = Column(DateTime)
    status_code = Column(
        SmallInteger, default=U_DEFAULT, server_default=str(U_DEFAULT))
    # foreign keys ############################################################
    # To delete article, first set url.article_id to NULL, then delete article
    article_id = Column(
        Integer,
        ForeignKey('article.id', ondelete='CASCADE', onupdate='CASCADE'))
    # site_id should be on delete cascade
    site_id = Column(
        Integer, ForeignKey('site.id', ondelete='CASCADE', onupdate='CASCADE'))
    # relationship attributes
    platforms = relationship(
        'Platform',
        passive_deletes=True,
        secondary='ass_url_platform',
        back_populates='urls')
    tweets = relationship(
        'Tweet',
        passive_deletes=True,
        secondary='ass_tweet_url',
        back_populates='urls')
    site = relationship('Site', back_populates='urls')


class Link(TableMixin, AuditColumns, Base):
    """Link table is used to log the links in one page.

    This table is a placeholder for future used (match feature).
    """
    to_raw = Column(String(MAX_URL_LEN), unique=True, nullable=False)
    to_canonical = Column(String(MAX_URL_LEN), nullable=False)
    to_site_id = Column(Integer)
    from_canonical = Column(String(MAX_URL_LEN))


class UrlMatch(TableMixin, AuditColumns, Base):
    """Table `url_match`

    Store matched pair of claim urls and fact checking urls. This table is a
    placeholder for future used.
    """
    claim_canonical = Column(String(MAX_URL_LEN), nullable=False)
    fact_checking_canonical = Column(String(MAX_URL_LEN), nullable=False)
    method = Column(String(255))
    match_or_not = Column(Boolean)
    __table_args__ = (UniqueConstraint(
        'claim_canonical',
        'fact_checking_canonical',
        name='claim_fact_checking_uq'), )


class Article(TableMixin, AuditColumns, Base):
    """Table `article` to record article from news site.

    This table has three stages:
    1) s1: (canonical_url, html, date_captured, site_id) is known, inserted
    from URL spiders, status_code=0.
    2) s2: article is parsed, so that title, meta and content are known, the
    status_code of these good records are A_P_SUCCESS.
    3) s3: article is indexed, indicated by meta_info table.

    Relations
    ---------
    article <-- ONE TO MANY --> url
    article <-- MANY TO ONE --> site
    article <-- ONE TO MANY --> article_version


    status_code : integer
        The status of this record.
    ---------------------------------------------------------------------------
    CONSTANT                 | VALUE |     DESCRIPTION
    ---------------------------------------------------------------------------
    A_DEFAULT                | 0     | Default, when inserting by URL spiders
    A_P_SUCCESS              | 20    | Article parse sucessfully
    A_WP_ERROR_DATA_INVALID  | 81    | Invalid data from web parser, e.g., Null
    A_WP_ERROR_UNKNOWN       | 82    | Unknown error when parsing article
    ---------------------------------------------------------------------------
    """
    canonical_url = Column(String(MAX_URL_LEN), unique=True, nullable=False)
    title = Column(String(255))
    meta = Column(postgresql.JSON)
    content = deferred(Column(Text))
    date_published = Column(DateTime)
    date_captured = Column(DateTime, nullable=False)
    html = deferred(Column(Text))
    group_id = Column(Integer)
    status_code = Column(
        SmallInteger, default=A_DEFAULT, server_default=str(A_DEFAULT))
    site_id = Column(
        Integer,
        ForeignKey('site.id', ondelete='CASCADE', onupdate='CASCADE'),
        nullable=False)
    __table_args__ = (Index('article_group_id', 'group_id'), )


class Site(TableMixin, AuditColumns, Base):
    """Table `site` to record site information.

    Relations
    ---------
    site <-- ONE TO MANY --> article
    site <-- ONE TO MANY --> urls
    """
    name = Column(String(255), unique=True, nullable=False)
    domain = Column(String(255), unique=True, nullable=False)
    site_type = Column(String(31), nullable=False)
    # JSON format ['t1', 't2']
    base_url = Column(String(511), unique=True, nullable=False)
    # JSON format ['a1.com', 'a2.com']
    article_rules = Column(postgresql.JSON)
    is_alive = Column(Boolean, default=True)
    is_enabled = Column(Boolean, default=True)
    last_alive = Column(DateTime, default=datetime.now)

    # relationship attributes
    site_tags = relationship(
        'SiteTag',
        passive_deletes=True,
        secondary='ass_site_site_tag',
        back_populates='sites')
    alternate_domains = relationship("AlternateDomain", back_populates='site')
    urls = relationship("Url", back_populates='site')


class AlternateDomain(TableMixin, AuditColumns, Base):
    """Table `alternate_domain`."""
    name = Column(String(255), unique=True, nullable=False)
    is_alive = Column(Boolean, default=True)
    site_id = Column(
        Integer, ForeignKey('site.id', ondelete='CASCADE', onupdate='CASCADE'))
    site = relationship("Site", back_populates='alternate_domains')


class SiteTag(TableMixin, AuditColumns, Base):
    """Table `site_tag`."""
    name = Column(String(32), nullable=False)
    source = Column(String(255), nullable=False)
    sites = relationship(
        'Site',
        passive_deletes=True,
        secondary='ass_site_site_tag',
        back_populates='site_tags')
    __table_args__ = (UniqueConstraint('name', 'source', name='name_source'), )


class SiteActivity(TableMixin, Base):
    """Table `site_activity` to log site activities.

    This table is a placeholder for future use.
    """
    event = Column(String(31))
    description = Column(String(255))
    timestamp = Column(DateTime, default=datetime.now)
    site_id = Column(
        Integer, ForeignKey('site.id', ondelete='CASCADE', onupdate='CASCADE'))


class Hashtag(TableMixin, AuditColumns, Base):
    """Table `hashtag`, hashtag used in tweet or other social networks.

    Relations
    ---------
    hashtag <-- MANY TO MANY --> tweet
    """
    text = Column(String(255), unique=True, nullable=False)
    # relationship attributes
    tweets = relationship(
        'Tweet',
        passive_deletes=True,
        secondary='ass_tweet_hashtag',
        back_populates='hashtags')


class Tweet(TableMixin, AuditColumns, Base):
    """Table `tweet` to record tweet we collect.

    Relations
    ---------
    tweet <-- MANY TO MANY --> url
    tweet <-- MANY TO MANY --> hashtag
    tweet <-- MANY TO ONE  --> tweet_user
    """
    raw_id = Column(BigInteger, unique=True, nullable=False)
    json_data = deferred(Column(postgresql.JSON, nullable=False))

    # foreign keys
    user_id = Column(
        Integer,
        ForeignKey('twitter_user.id', ondelete='CASCADE', onupdate='CASCADE'))
    # relationship attributes
    urls = relationship(
        'Url',
        passive_deletes=True,
        secondary='ass_tweet_url',
        back_populates='tweets')
    hashtags = relationship(
        'Hashtag',
        passive_deletes=True,
        secondary='ass_tweet_hashtag',
        back_populates='tweets')
    user = relationship('TwitterUser', back_populates='tweets')


class AssTweet(TableMixin, Base):
    """Table `ass_tweet`.

    Association tweet with its replies, quotes and retweets.
    """
    retweeted_status_id = Column(BigInteger, index=True)
    quoted_status_id = Column(BigInteger, index=True)
    in_reply_to_status_id = Column(BigInteger, index=True)


class TwitterUser(TableMixin, Base):
    """Table `twitter_user` to record user that sends tweets.

    Relations
    ---------
    twitter_user <--ONE TO MANY --> tweet
    """
    # foreign keys
    raw_id = Column(BigInteger, unique=True, nullable=False)

    # relationship attribute
    tweets = relationship('Tweet', passive_deletes=True, back_populates='user')


class TwitterUserUnion(TableMixin, Base):
    """A Union set of all related Twitter users, including
    original, the user who is original tweeter;
    retweeter, the user who retweets it;
    replier, the user who replies it;
    quoter, the user who quotes it; and
    mentioner, the user who is mentioned;

    This super user table save not only more users, but also more attributes
    about the users. Please note that
    1) for cases like original, retweeter, quoter, the profile of these user
       is updated; for mentioner, the profile need to fetch if not exist
    2) it is not guaranteed that the attributes of this user is the newest,
       we can only say that it is the newest from the tweets we collected.
    """
    raw_id = Column(BigInteger, unique=True, nullable=False)
    screen_name = Column(String(255), nullable=False)
    followers_count = Column(Integer)
    profile = deferred(Column(postgresql.JSONB))
    updated_at = Column(DateTime, onupdate=datetime.utcnow)
    user_error_code = Column(Integer)


class TwitterNetworkEdge(TableMixin, Base):
    """Edges of the information diffusion network in Twitter."""
    tweet_raw_id = Column(BigInteger, nullable=False)
    from_raw_id = Column(BigInteger, nullable=False)
    to_raw_id = Column(BigInteger, nullable=False)
    url_id = Column(Integer, nullable=False)
    # Indicating whether the url is in the quoted_status
    is_quoted_url = Column(Boolean, nullable=False)
    is_mention = Column(Boolean, nullable=False)
    tweet_type = Column(String(31), nullable=False)
    __table_args__ = (UniqueConstraint(
        'tweet_raw_id',
        'from_raw_id',
        'to_raw_id',
        'url_id',
        'is_quoted_url',
        'is_mention',
        'tweet_type',
        name='uq_edge'), )


class MetaInfo(TableMixin, AuditColumns, Base):
    """Table `meta_info` to record meta values.

    All values should be casted into string format before save into database.
    When retrieve you have to transfer the value back.
    """
    name = Column(String(255), unique=True, nullable=False)
    value = Column(String(255), nullable=False)
    value_type = Column(String(15), nullable=False)
    description = Column(String(255))

    def get_value(self):
        """Get the original values."""
        if self.value_type == "str":
            return self.value
        elif self.value_type == 'int':
            return int(self.value)
        elif self.value_type == 'bool':
            if self.value.lower() in ('true', 't', 'y', 'yes'):
                return True
            elif self.value.lower() in ('false', 'f', 'n', 'no'):
                return False
            else:
                return bool(self.value)
        elif self.value_type == 'float':
            return float(self.value)
        elif self.value_type == 'datetime':
            return dateutil.parser.parse(self.value)
        else:
            logger.error('Unsupported meta value_type %s', self.value_type)
            return None

    def set_value(self, raw_value):
        """Set the string type values."""
        self.value = str(raw_value)


class Top20SpreaderMonthly(TableMixin, Base):
    """The top spreaders in 30 days window."""
    upper_day = Column(Date, nullable=False)
    user_id = Column(Integer, nullable=False)
    user_raw_id = Column(BigInteger, nullable=False)
    user_screen_name = Column(String(255), nullable=False)
    site_type = Column(String(255), nullable=False)
    spreading_type = Column(String(255), nullable=False)
    number_of_tweets = Column(Integer, nullable=False)
    bot_or_not = Column(postgresql.JSON)
    __table_args__ = (UniqueConstraint(
        'upper_day',
        'user_id',
        'site_type',
        'spreading_type',
        name='spreading_uq'), )


class Top20ArticleMonthly(TableMixin, Base):
    """The top articles in 30 days window."""
    upper_day = Column(Date, nullable=False)
    date_captured = Column(DateTime, nullable=False)
    title = Column(String(255), nullable=False)
    canonical_url = Column(String(MAX_URL_LEN), nullable=False)
    site_type = Column(String(255), nullable=False)
    number_of_tweets = Column(Integer, nullable=False)
    __table_args__ = (UniqueConstraint(
        'upper_day', 'canonical_url', name='top20_article_uq'), )
