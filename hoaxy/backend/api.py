# -*- coding: utf-8 -*-
"""Implemention of hoaxy backend APIs

(1) Mashape framework is used for deployment.
(2) Flask framework is used for implementation of APIs

"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

from datetime import datetime
from datetime import timedelta
from flask import Flask, request
from hoaxy import CONF
from hoaxy.database import ENGINE as engine
from hoaxy.database.models import MetaInfo
from hoaxy.database.models import N_CLAIM, N_FACT_CHECKING
from hoaxy.exceptions import APINoResultError
from hoaxy.exceptions import APIParseError
from hoaxy.ir.search import db_query_network
from hoaxy.ir.search import db_query_filter_disabled_site
from hoaxy.ir.search import db_query_latest_articles
from hoaxy.ir.search import db_query_top_articles
from hoaxy.ir.search import db_query_top_spreaders
from hoaxy.ir.search import db_query_tweets
from hoaxy.ir.search import db_query_twitter_shares
from hoaxy.ir.search import Searcher
from hoaxy.utils.log import configure_logging
from schema import Schema, Optional, And, Use, Regex, Or
from schema import SchemaError
from sqlalchemy.exc import SQLAlchemyError
import dateutil
import flask
import functools
import logging
import lucene
import sqlalchemy


logger = logging.getLogger(__name__)

# Flask app instance configuration of mashape authentication
app = Flask(__name__)
app.config['MASHAPE_SECRET'] = CONF['api']['mashape']['secret']
app.config['MASHAPE_IPS'] = CONF['api']['mashape']['ips']

# Prepare EasyLuceneSearcher class
searcher = Searcher(index_dir=CONF['lucene']['index_dir'],
                    boost=CONF['lucene']['boost'])
# API Settings
REFRESH_INTERVAL = timedelta(**CONF['api']['searcher_refresh_interval'])
TO_JSON_KWARGS = CONF['api']['dataframe_to_json_kwargs']
N1 = CONF['api']['n_query_api_returned']
N2 = CONF['api']['n_query_of_recent_sorting']
MIN_SCORE = CONF['api']['min_score_of_recent_sorting']


def streaming_start_at(engine):
    """Return starting datetime of twitter tracking.

    Parameters
    ----------
    engine : sqlalchemy connection
        The database connection, e.g., engine or session.

    Returns
    -------
    datetime
        The created_at datetime of the first tweet in the database.
    """
    q = 'SELECT created_at FROM tweet ORDER BY id ASC LIMIT 1'
    return engine.execute(q).scalar()


# Starting datetime of twitter streaming
STRAMING_START_AT = streaming_start_at(engine)


def copy_req_args(req_args):
    """Replicate request.args into a dict.

    Parameters
    ----------
    req_args : MultiDict
        Flask `request.args`.
    Returns
    -------
    dict
        A replication of `req_args`.
    """
    q_kwargs = dict()
    for k, v in req_args.iteritems():
        q_kwargs[k] = v
    return q_kwargs


def authenticate_mashape(func):
    """Decorator to authenticate request with Mashape."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Mashape authentication
        mashape_secret = request.headers.get('X-Mashape-Proxy-Secret')
        if mashape_secret is not None:
            client_ip = request.access_route[-1]
            if (client_ip in app.config['MASHAPE_IPS'] and
                    mashape_secret == app.config['MASHAPE_SECRET']):
                return func(*args, **kwargs)
        # No authentication
        return "Invalid/expired token", 401
    return wrapper


@app.before_first_request
def setup_logging():
    """Before first request, set up logger."""
    configure_logging('api', file_level='WARNING')


@app.before_request
def before_request():
    """Before request, try to refresh content of searcher."""
    now_dt = datetime.utcnow()
    now_str = now_dt.strftime('%Y-%m-%d %H:%M:%S')
    q0 = "SELECT value from meta_info WHERE name='searcher_refresh_date'"
    q1 = "UPDATE meta_info SET value=:now WHERE name='searcher_refresh_date'"
    try:
        recent_refresh = engine.execute(q0).scalar()
    except SQLAlchemyError:
        recent_refresh = engine.execute(q0).scalar()
    if recent_refresh:
        recent_refresh = dateutil.parser.parse(recent_refresh)
        if now_dt - recent_refresh > REFRESH_INTERVAL:
            searcher.refresh()
            engine.execute(sqlalchemy.text(q1), now=now_str)
    else:
        engine.execute(MetaInfo.__table__.insert().values(
            name='searcher_refresh_date',
            value=now_str,
            value_type='datetime',
            description="in api, recent refresh datetime of searcher"))


@app.route('/')
@authenticate_mashape
def verify_mashape():
    """Verify backend secret and client token are correct.

    When decorated with @authenticate_mashape, this verifies that the Mashape
    config is correct and that the client's token is good.

    Returns
    -------
    tuple
        If authentication success, return HTTP 200.
    """
    return 'OK', 200


@app.route('/articles')
@authenticate_mashape
def query_articles():
    """Handle API request '/articles'.

    API Request Parameters
    ----------------------
        query : string
        sort_by : {'relevant', 'recent'}
        use_lucene_syntax : bool

    API Response Keys
    -----------------
        status : string
        num_of_entries : int
        total_hits : int
        articles : dict
            keys are:
                canonical_url : string
                date_published : string formatted datetime
                domain : string
                id : int
                number_of_tweets : int
                score : float
                site_type : {'claim', 'fact_checking'}
                title : string

    """
    lucene.getVMEnv().attachCurrentThread()
    # Validate input of request
    q_articles_schema = Schema({
        'query': lambda s: len(s) > 0,
        Optional('sort_by', default='relevant'):
            And(unicode, lambda s: s in ('relevant', 'recent')),
        Optional('use_lucene_syntax', default=True): And(
            unicode, Use(lambda s: s.lower()),
            lambda s: s in ('true', 'false'),
            Use(lambda s: True if s == 'true' else False)),
    })
    q_kwargs = copy_req_args(request.args)
    try:
        q_kwargs = q_articles_schema.validate(q_kwargs)
        n, df = searcher.search(n1=N1, n2=N2,
                                min_score_of_recent_sorting=MIN_SCORE,
                                min_date_published=STRAMING_START_AT,
                                **q_kwargs)
        df = db_query_filter_disabled_site(engine, df)
        df = db_query_twitter_shares(engine, df)
        if len(df) == 0:
            raise APINoResultError('No article found!')
        # sort dataframe by 'number_of_tweets'
        df = df.sort_values('number_of_tweets', ascending=False)
        response = dict(
            status='OK',
            num_of_entries=len(df),
            total_hits=n,
            articles=flask.json.loads(df.to_json(**TO_JSON_KWARGS)))
    except SchemaError as e:
        response = dict(status='Parameter error', error=str(e))
    except APIParseError as e:
        response = dict(status='Invalide query', error=str(e))
    except APINoResultError as e:
        response = dict(status='No result error', error=str(e))
    except Exception as e:
        logger.exception(e)
        response = dict(status='ERROR', error='Server error, query failed!')
    return flask.jsonify(response)


@app.route('/latest-articles')
@authenticate_mashape
def query_latest_articles():
    """Handle API request '/latest-articles'.

    API Request Parameters
    ----------------------
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

    API Response Keys
    -----------------
        status : string
        num_of_entries : int
        articles : dict
            keys are:
                canonical_url : string
                date_published : string formatted datetime
                domain : string
                id : int
                site_type : {'claim', 'fact_checking'}
                title : string
    """
    lucene.getVMEnv().attachCurrentThread()
    # Validate input of request
    q_articles_schema = Schema({
        'past_hours':
            And(Use(int), lambda x: x > 0,
                error='Invalid value of `past_hours`'),
        Optional('domains', default=None):
            Or(
                lambda s: s in ('fact_checking', 'claim', 'fake'),
                Use(flask.json.loads,
                    error='Not valid values nor JSON string of `domains`')
            )
    })
    q_kwargs = copy_req_args(request.args)
    try:
        q_kwargs = q_articles_schema.validate(q_kwargs)
        domains_file = CONF['api'].get('selected_fake_domains_path')
        df = db_query_latest_articles(engine, domains_file=domains_file,
                                      **q_kwargs)
        if len(df) == 0:
            raise APINoResultError('No articles found!')
        response = dict(
            status='OK',
            num_of_entries=len(df),
            articles=flask.json.loads(df.to_json(**TO_JSON_KWARGS)))
    except SchemaError as e:
        response = dict(status='Parameter error', error=str(e))
    except APIParseError as e:
        response = dict(status='Invalide query', error=str(e))
    except APINoResultError as e:
        response = dict(status='No result error', error=str(e))
    except Exception as e:
        logger.exception(e)
        response = dict(status='ERROR', error='Server error, query failed!')
    return flask.jsonify(response)


@app.route('/tweets')
@authenticate_mashape
def query_tweets():
    """Handle API '/tweets'.

    API Request Parameters
    ----------------------
        ids : list of int

    API Response Keys
    -----------------
        status : string
        num_of_entries : int
        tweets : dict
            canonical_url : string
            domain : string
            id : int
            date_published : string formatted datetime
            site_type : {'claim', 'fact_checking'}
            title : string
            tweet_created_at : string formatted datetime
            tweet_id : string
    """
    lucene.getVMEnv().attachCurrentThread()
    q_tweets_schema = Schema({
        'ids': And(Use(flask.json.loads, error="Format error of `ids`"),
                   lambda s: len(s) > 0, error='Empty of `ids`'
                   ),
    })
    q_kwargs = copy_req_args(request.args)
    try:
        q_kwargs = q_tweets_schema.validate(q_kwargs)
        df = db_query_tweets(engine, q_kwargs['ids'])
        if len(df) == 0:
            raise APINoResultError('No tweet found!')
        response = dict(status='OK',
                        num_of_entries=len(df),
                        tweets=flask.json.loads(df.to_json(**TO_JSON_KWARGS)))
    except SchemaError as e:
        response = dict(status='ERROR', error=str(e))
    except APINoResultError as e:
        response = dict(status='No result error', error=str(e))
    except Exception as e:
        logger.exception(e)
        response = dict(status='ERROR', error='Server error, query failed')
    return flask.jsonify(response)


@app.route('/timeline')
@authenticate_mashape
def query_timeline():
    """Handle API '/timeline'.

    API Request Parameters
    ----------------------
        ids : list of int
        resolution : character in 'HDWM'

    API Response Keys
    -----------------
        status : string
        timeline : dict
            claim : dict
                timestamp : list of string formatted datetime
                volume : list of int
            fact_checking : dict
                timestamp : list of string formatted datetime
                volume : list of int
    """
    lucene.getVMEnv().attachCurrentThread()
    q_tweets_schema = Schema({
        'ids': And(Use(flask.json.loads, error="Format error of `ids`"),
                   lambda s: len(s) > 0, error='Empty of `ids`'
                   ),
        Optional('resolution', default='D'): And(
            Use(lambda s: s.upper()), lambda s: s in 'HDWM'),
    })

    q_kwargs = copy_req_args(request.args)
    try:
        q_kwargs = q_tweets_schema.validate(q_kwargs)
        rule = '1' + q_kwargs.pop('resolution')
        df = db_query_tweets(engine, q_kwargs['ids'])
        if len(df) == 0:
            raise APINoResultError('No tweet found!')
        df = df.set_index('tweet_created_at')
        df1 = df.loc[df['site_type'] == N_FACT_CHECKING]
        s1 = df1['tweet_id'].drop_duplicates()
        s1 = s1.resample(rule).count()
        df2 = df.loc[df['site_type'] == N_CLAIM]
        s2 = df2['tweet_id'].drop_duplicates()
        s2 = s2.resample(rule).count()
        s1, s2 = s1.align(s2, join='outer', fill_value=0)
        s1 = s1.cumsum()
        s2 = s2.cumsum()
        response = dict(status='OK',
                        timeline=dict(
                            fact_checking=dict(
                                timestamp=s1.index.strftime(
                                    '%Y-%m-%dT%H:%M:%SZ').tolist(),
                                volume=s1.tolist()),
                            claim=dict(
                                timestamp=s2.index.strftime(
                                    '%Y-%m-%dT%H:%M:%SZ').tolist(),
                                volume=s2.tolist())))
    except SchemaError as e:
        response = dict(status='ERROR', error=str(e))
    except APINoResultError as e:
        response = dict(status='No result error', error=str(e))
    except Exception as e:
        logger.exception(e)
        response = dict(status='ERROR', error='Server error, query failed')
    return flask.jsonify(response)


@app.route('/network')
@authenticate_mashape
def query_network():
    """Handle API request '/network'.

    API Request Parameters
    ----------------------
        ids : list of int
        nodes_limit : int
        edges_limit : int
        include_user_mentions : bool

    API Response Keys
    -----------------
        status : string
        num_of_entries : int
        edges : dict
            canonical_url : string
            date_published : string formatted datetime
            domain : string
            from_user_id : string
            from_user_screen_name : string
            id : int
            is_mention : bool
            site_type : {'claim', 'fact_checking'}
            title : string
            to_user_id : string
            to_user_screen_name : string
            tweet_created_at : string formatted datetime
            tweet_id: string
            tweet_type: {'origin', 'retweet', 'quote', 'reply'}
    """
    lucene.getVMEnv().attachCurrentThread()
    q_network_schema = Schema({
        'ids': Use(flask.json.loads),
        Optional('nodes_limit', default=1000): And(Use(int), lambda i: i > 0),
        Optional('edges_limit', default=12500): And(Use(int), lambda i: i > 0),
        Optional('include_user_mentions', default=True): And(
            unicode, Use(lambda s: s.lower()),
            lambda s: s in ('true', 'false'),
            Use(lambda s: True if s == 'true' else False)),
    })
    q_kwargs = copy_req_args(request.args)
    try:
        q_kwargs = q_network_schema.validate(q_kwargs)
        df = db_query_network(engine, **q_kwargs)
        if len(df) == 0:
            raise APINoResultError('No edge could be built!')
        response = dict(status='OK',
                        num_of_entries=len(df),
                        edges=flask.json.loads(df.to_json(**TO_JSON_KWARGS)))
    except SchemaError as e:
        response = dict(status='ERROR', error=str(e))
    except APINoResultError as e:
        response = dict(status='No result error', error=str(e))
    except Exception as e:
        logger.exception(e)
        response = dict(status='ERROR', error='Server error, query failed')
    return flask.jsonify(response)


@app.route('/top-users')
@authenticate_mashape
def query_top_spreaders():
    """Handle API request '/top-user'.

    API Request Parameters
    ----------------------
        upper_day : string formatted datetime
        most_recent : bool

    API Response Keys
    -----------------
        status : string
        num_of_entries : int
        spreaders : dict
            bot_score : float
            number_of_tweets : int
            site_type : {'claim', 'fact_checking'}
            spreading_type : {'active', 'influencial'}
            upper_day : string formatted datetime
            user_id : int
            user_raw_id : string
            user_screen_name : string

    """
    lucene.getVMEnv().attachCurrentThread()
    yesterday = datetime.utcnow().date() - timedelta(days=1)
    yesterday = yesterday.strftime('%Y-%m-%d')

    q_top_spreaders_schema = Schema({
        Optional('upper_day', default=yesterday): And(
            Regex('^\d{4}-\d{2}-\d{2}$'),
            Use(dateutil.parser.parse),
            error='Invalid date, should be yyyy-mm-dd format'),
        Optional('most_recent', default=True): And(
            unicode, Use(lambda s: s.lower()),
            lambda s: s in ('true', 'false'),
            Use(lambda s: True if s == 'true' else False)),
    })
    q_kwargs = copy_req_args(request.args)
    try:
        q_kwargs = q_top_spreaders_schema.validate(q_kwargs)
        df = db_query_top_spreaders(engine, **q_kwargs)
        if len(df) == 0:
            raise APINoResultError('No top spreader found!')
        response = dict(
            status='OK',
            num_of_entries=len(df),
            spreaders=flask.json.loads(df.to_json(**TO_JSON_KWARGS)))
    except SchemaError as e:
        response = dict(status='ERROR', error=str(e))
    except APINoResultError as e:
        response = dict(status='No result error', error=str(e))
    except Exception as e:
        logger.exception(e)
        response = dict(status='ERROR', error='Server error, query failed')
    return flask.jsonify(response)


@app.route('/top-articles')
@authenticate_mashape
def query_top_articles():
    """Handle API request 'top-articles'

    API Request Parameters
    ----------------------
        upper_day : string formatted datetime
        most_recent : bool

    API Response Keys
    -----------------
        status : string
        num_of_entries : int
        articles : dict
            canonical_url : string
            date_captured : string formatted datetime
            number_of_tweets : int
            site_type : {'claim', 'fact_checking'}
            title : string
            upper_day : string formatted datetime
    """
    lucene.getVMEnv().attachCurrentThread()
    yesterday = datetime.utcnow().date() - timedelta(days=1)
    yesterday = yesterday.strftime('%Y-%m-%d')

    q_top_article_schema = Schema({
        Optional('upper_day', default=yesterday): And(
            Regex('^\d{4}-\d{2}-\d{2}$'),
            Use(dateutil.parser.parse),
            error='Invalid date, shoul be yyyy-mm-dd format'),
        Optional('most_recent', default=True): And(
            unicode, Use(lambda s: s.lower()),
            lambda s: s in ('true', 'false'),
            Use(lambda s: True if s == 'true' else False)),
    })
    q_kwargs = copy_req_args(request.args)
    try:
        q_kwargs = q_top_article_schema.validate(q_kwargs)
        df = db_query_top_articles(engine, **q_kwargs)
        if len(df) == 0:
            raise APINoResultError('No top article found!')
        response = dict(
            status='OK',
            num_of_entries=len(df),
            articles=flask.json.loads(df.to_json(**TO_JSON_KWARGS)))
    except SchemaError as e:
        response = dict(status='ERROR', error=str(e))
    except APINoResultError as e:
        response = dict(status='No result error', error=str(e))
    except Exception as e:
        logger.exception(e)
        response = dict(status='ERROR', error='Server error, query failed')
    return flask.jsonify(response)
