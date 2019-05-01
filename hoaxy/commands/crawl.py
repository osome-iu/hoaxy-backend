# -*- coding: utf-8 -*-
"""Hoaxy subcommand CrawlCmd implementation.

All crawl processes is called by this commands. See CrawlCmd class.

"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

from hoaxy.commands import HoaxyCommand
from hoaxy.crawl.spiders import build_spiders_iter
from hoaxy.crawl.spiders.article import ArticleParserSpider
from hoaxy.crawl.spiders.html import HtmlSpider
from hoaxy.database import Session
from hoaxy.database.functions import get_msites
from hoaxy.database.functions import get_platform_id
from hoaxy.database.models import DEFAULT_WHERE_EXPR_FETCH_HTML
from hoaxy.database.models import DEFAULT_WHERE_EXPR_FETCH_URL
from hoaxy.database.models import DEFAULT_WHERE_EXPR_PARSE_ARTICLE
from hoaxy.database.models import N_PLATFORM_WEB
from hoaxy.database.models import Site
from hoaxy.database.models import Url
from hoaxy.utils.log import configure_logging
from schema import Schema, Use, Or
from schema import SchemaError
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from sqlalchemy import text
import logging
import re
import sys

logger = logging.getLogger(__name__)
HTTP_TIMEOUT = 30  # HTTP REQUEST TIMEOUT, IN SECONDS
# regular expression to match domain
DOMAIN_RE = re.compile('^([a-z0-9]+(-[a-z0-9]+)*\.)+[a-z]{2,}$')
REQ_FIELDS = ('name', 'domain', 'site_type')


def build_order_by_expr(ob_cls, ob_kw):
    """Internal method to build SQLAlchemy order-by expression from dict.

    Parameters
        ob_cls : class
            database models.
        ob_kw : dict
            a dict to represent order-by {'col': 'order'}.
    """
    if ob_kw is None or len(ob_kw) == 0:
        return None
    elif len(ob_kw) == 1:
        for k, v in ob_kw.items():
            if v.lower() == 'asc':
                return getattr(ob_cls, k).asc()
            elif v.lower() == 'desc':
                return getattr(ob_cls, k).desc()
            else:
                print(('Invalidate order keyword: %s', ob_kw))
                sys.exit(2)
    else:
        print(('Support one column order by only: %s', ob_kw))
        sys.exit(2)


class CrawlCmd(HoaxyCommand):
    """
Usage:
  hoaxy crawl --fetch-url (--update | --archive)
              [--where-expr=<w> --order-by=<o> --limit=<l>]
  hoaxy crawl --fetch-html [--where-expr=<w> --order-by=<o> --limit=<l>]
  hoaxy crawl --parse-article [--where-expr=<w> --order-by=<o> --limit=<l>]

This command provides crawling functions, including fetching URLs from sites,
fetching HTMLs of URLs, and parsing HTML pages into structured article data.

When crawling, you can specify which entity to fetch:
--fetch-url             Fetch URL, either news update or archive.
--fetch-html            Fetch HTML of URL.
--parse-article         Parse HTML page into structured article data.
                          At this moment, we use webparse provided by
                          https://mercury.postlight.com/web-parser/

We suggest you add `--limit` option when doing cronjobs to avoid overload.

When fetching URL, you can specify which kind of URLs to fetch by option:
--updated               Fetch news update (the newest articles).
--archive               Fetch news archive (the whole site articles). Be
                          careful to use this option, it is time consuming.

The inputs for crawling processes (domains or URLs) are queried from DB. By
default you do not need to consider the internal SQL query. However,
if you want to specify your own data collection for crawling in some cases,
you can use these options:
--where-expr=<w>       The WHERE expression for SQL query.
--order-by=<o>         The sorted order of `id` column, by default ascending
                         [default: asc]
--limit=<l>            The limit expression, by default return all

For --fetch-url, the default raw SQL query is on table `site`:
    site.is_enabled is True and site.is_alive is True
For --fetch-html the default raw SQL query is on table `url`
    url.status_code=U_DEFAULT
For --parse-article the default raw SQL query is on table `url`
    url.status_code=U_HTML_SUCCESS AND url.site_id IS NOT NULL

Please check database.models to see detail.

Other options are:
-h --help               Show help.

More information:
(1) The spiders are built base on scrapy package (https://scrapy.org/)
(2) To distinguish URL records from these three phrase, we use `status_code`
    column, and will update the column for each action. Please check the Url
    model to see the table of status_code
(3) Parse a HTML page into article is a difficult task. We don't implement it
    ourselves. Instead we use the service from https://mercury.postlight.com/.
    However, you can also use other parser, e.g., python-goose,
    https://github.com/grangier/python-goose, or even implement your own.

Examples (`||` represents continue of commands, you can ignore when using):
    1. Most often you would like to fetch the updates of all enabled sites,
       probably used as cronjob:
        hoaxy crawl --fetch-url --update

    2. If you would like to fetch the URLs of old posts (archive):
        hoaxy crawl --fetch-url --archive
       Note: this crawling is quite time-consuming.

    3. Also you may need structured article instead of HTML documents:
        hoaxy crawl --parse-article

    4. When automating by crontab, you may need --limit option to limit
       the number of entries to process. This is very useful when you
       already have a huge number of URLs in the database, and you would
       like to consume them each time a block.

        hoaxy crawl --fetch-html --limit=20000

    5. Occasionally, you can specify the where expression to crawl only on
       these entries:
        hoaxy crawl --parse-article --where-expr='url.html is null'
    """
    name = 'crawl'
    short_description = 'Crawl news sites'
    args_schema = Schema({
        '--order-by':
        Or(None,
           Use(str.lower),
           lambda s: s in ('asc', 'desc'),
           error='must be asc or desc'),
        '--limit':
        Or(None, Use(int)),
        object:
        object
    })

    @classmethod
    def fetch_url(cls, session, msites, platform_id, purpose):
        """Actual method to do fetch url action.

        Parameters
        ----------
            msites : list
                a list of Site model class, contains info to build spiders.
            platform_id : int
                id of platform, bind fetched url with this id.
            purpose : {'update', 'archive'}
                indicate which url to fetch.
        """
        settings = Settings(cls.conf['crawl']['scrapy'])
        settings.set('ITEM_PIPELINES',
                     {'hoaxy.crawl.pipelines.UrlPipeline': 300})
        process = CrawlerProcess(settings)
        sll = cls.conf['logging']['loggers']['scrapy']['level']
        logging.getLogger('scrapy').setLevel(logging.getLevelName(sll))
        for ms in msites:
            for sm in build_spiders_iter(ms, purpose):
                sm['kwargs']['session'] = session
                sm['kwargs']['platform_id'] = platform_id
                process.crawl(sm['cls'], *sm['args'], **sm['kwargs'])
        process.start()

    @classmethod
    def fetch_html(cls, session, url_tuples):
        """Actual method to do fetch html action.

        Parameters
        ----------
            session : object
                a SQLAlchemy session object.
            url_tuples : list
                a list of url tuple (id, raw, status_code).
        """
        settings = Settings(cls.conf['crawl']['scrapy'])
        settings.set('ITEM_PIPELINES',
                     {'hoaxy.crawl.pipelines.HtmlPipeline': 300})
        process = CrawlerProcess(settings)
        sll = cls.conf['logging']['loggers']['scrapy']['level']
        logging.getLogger('scrapy').setLevel(logging.getLevelName(sll))
        logger.warning('Number of url to fetch html is: %s', len(url_tuples))
        process.crawl(
            HtmlSpider,
            session=session,
            url_tuples=url_tuples,
            excluded_domains=cls.conf['crawl']['excluded_domains'])
        process.start()

    @classmethod
    def parse_article(cls, session, url_tuples):
        """Actual method to do parse to article action.

        Parameters
        ----------
            session : object
                a SQLAlchemy session object.
            url_tuples : list
                a list of url tuple (id, created_at, date_published,
                canonical, site_id)
        """
        settings = Settings(cls.conf['crawl']['scrapy'])
        settings.set('ITEM_PIPELINES',
                     {'hoaxy.crawl.pipelines.ArticlePipeline': 300})
        process = CrawlerProcess(settings)
        sll = cls.conf['logging']['loggers']['scrapy']['level']
        logging.getLogger('scrapy').setLevel(logging.getLevelName(sll))
        logger.info('Number of url to parse is: %s', len(url_tuples))
        process.crawl(
            ArticleParserSpider,
            session=session,
            url_tuples=url_tuples,
            node_path=cls.conf['crawl']['article_parser']['node_installation_path'],
            mercury_parser_path=cls.conf['crawl']['article_parser']['parse_with_mercury_js_path'],
        )
        process.start()

    @classmethod
    def run(cls, args):
        """Overriding method as the entry point of this command."""
        try:
            args = cls.args_schema.validate(args)
        except SchemaError as e:
            raise SystemExit(e)

        session = Session(expire_on_commit=False)
        # session = Session()
        where_expr = args['--where-expr']
        ob_expr = args.get('--order-by', 'asc')
        limit = args['--limit']
        # --fetch-url
        if args['--fetch-url'] is True:
            configure_logging(
                'crawl.fetch-url',
                console_level=args['--console-log-level'],
                file_level='WARNING')
            purpose = 'update' if args['--update'] is True else 'archive'
            if where_expr is None:
                where_expr = [text(DEFAULT_WHERE_EXPR_FETCH_URL)]
            else:
                where_expr = [text(where_expr)]
            ob_expr = Site.id.asc() if ob_expr == 'asc' else Site.id.desc()
            msites = get_msites(
                session, f_expr=where_expr, ob_expr=ob_expr, limit=limit)
            if len(msites) == 0:
                logger.warning("None sites you queried found in DB!")
                raise SystemExit(2)
            platform_id = get_platform_id(session, name=N_PLATFORM_WEB)
            # detach msites and mplatform from session,
            # since they definitely would not be modified in session
            for ms in msites:
                session.expunge(ms)
            logger.warning('Starting crawling process to fetch URL update ...')
            cls.fetch_url(session, msites, platform_id, purpose)
        elif args['--fetch-html'] is True:
            configure_logging(
                'crawl.fetch-html',
                console_level=args['--console-log-level'],
                file_level='WARNING')
            if not session.query(Site.id).count() > 0:
                raise SystemExit('Your site table is empty!')
            q = session.query(Url.id, Url.raw)
            if where_expr is None:
                where_expr = [text(DEFAULT_WHERE_EXPR_FETCH_HTML)]
            else:
                where_expr = [text(where_expr)]
            ob_expr = Url.id.asc() if ob_expr == 'asc' else Url.id.desc()
            q = q.filter(*where_expr).order_by(ob_expr)
            if limit is not None:
                q = q.limit(limit)
            logger.info(
                q.statement.compile(compile_kwargs={"literal_binds": True}))
            url_tuples = q.all()
            if not url_tuples:
                logger.warning('No such URLs in DB!')
                raise SystemExit(2)
            logger.warning('Staring crawling process to fetch HTML ...')
            cls.fetch_html(session, url_tuples)
        # --parse-article
        elif args['--parse-article'] is True:
            configure_logging(
                'crawl.parse-article',
                console_level=args['--console-log-level'],
                file_level='WARNING')
            q = session.query(Url.id, Url.created_at, Url.date_published,
                              Url.canonical, Url.site_id)
            if where_expr is None:
                where_expr = [text(DEFAULT_WHERE_EXPR_PARSE_ARTICLE)]
            else:
                where_expr = [text(where_expr)]
            ob_expr = Url.id.asc() if ob_expr == 'asc' else Url.id.desc()
            q = q.filter(*where_expr).order_by(ob_expr)
            if limit is not None:
                q = q.limit(limit)
            logger.info(
                q.statement.compile(compile_kwargs={"literal_binds": True}))
            url_tuples = q.all()
            if not url_tuples:
                logger.warning('No URLs found from DB!')
                raise SystemExit(2)
            logger.warning('Starting crawling process to parse article ...')
            cls.parse_article(session, url_tuples)
        session.close()
