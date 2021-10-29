# -*- coding: utf-8 -*-
"""ArticleParser implementation.

We use webparser from https://mercury.postlight.com/ to parse a URL into
structured article.
"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

from hoaxy.crawl.items import ArticleItem
from hoaxy.database.models import Article
from hoaxy.database.models import U_WP_ERROR_DATA_INVALID
from hoaxy.database.models import U_WP_ERROR_UNKNOWN
from hoaxy.database.models import U_WP_SUCCESS
from hoaxy.database.models import Url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import load_only
import json
import logging
import lxml.html
import scrapy
import subprocess
from newspaper import Article as npArticle
from hoaxy.utils.dt import utc_from_str
from hoaxy.utils.log import configure_logging
from datetime import datetime
import demjson
from requests.utils import quote
import jieba

jieba.setLogLevel(20)
logger = logging.getLogger(__name__)


class ArticleParserSpider(scrapy.spiders.Spider):
    """A web news article parser spider.

    The service is provided by https://mercury.postlight.com/. This spider
    build requests, and send them to the service and parse the data
    and insert into our article table.
    """
    name = 'article_parser.spider'

    def __init__(self, session, url_tuples, *args, **kwargs):
        """Constructor of ArticleParserSpider.

        Parameters
        ----------
        session : obj
            A SQLAlchemy session instance.
        url_tuples : list
            A list of tuple (id, created_at, date_published,
            canonical_url, site_id), which is a URL collection fetched from
            database.
        node_path : string
            node executable path.
        mercury_parser_installation_path : string
            pwd of <hoaxy-backened>/hoaxy/node_scripts/parse_with_mercury.js.
        """
        self.session = session
        self.url_tuples = url_tuples
        self.node_path = kwargs.pop('node_path')
        self.mercury_parser_installation_path = kwargs.pop('mercury_parser_path')
        configure_logging(
            'crawl.parse-article',
            console_level='CRITICAL',
            file_level='WARNING')
        super(ArticleParserSpider, self).__init__(*args, **kwargs)

    def is_url_parsed(self, *url_tuple):
        """Test whether this URL is parsed.

        Before parse the article, test whether this URL is parsed before.
        If so, ignore and return True. Otherwise, need to parse, return False.

        Parameters
        ----------
        url_tuple : tuple
            Tuple (url_id, created_at, canonical)

        Returns
        -------
        bool
            Whether this URL is parsed or not.
        """
        url_id, created_at, canonical = url_tuple
        marticle = self.session.query(Article)\
            .filter_by(canonical_url=canonical)\
            .options(load_only('id', 'date_captured'))\
            .one_or_none()
        # marticle exists
        # update date_captured of this article
        if marticle is not None:
            # marticle.date_captured > article['date_captured']
            if marticle.date_captured > created_at:
                marticle.date_captured = created_at
            try:
                self.session.query(Url).filter_by(id=url_id).update(
                    dict(article_id=marticle.id, status_code=U_WP_SUCCESS))
                self.session.commit()
                return True
            except SQLAlchemyError as e:
                logger.error('Error when update url: %s', e)
                raise
        else:
            return False

    def start_requests(self):
        for url_id, created_at, date_published, canonical,\
                site_id in self.url_tuples:
            if canonical is None:
                logger.error("""The canonical format of URL 'id=%s' \
is not determined""", url_id)
                continue
            if self.is_url_parsed(url_id, created_at, canonical) is True:
                logger.info("URL %r was parsed before.", canonical)
                continue
            url = canonical.encode('utf-8', errors='ignore')
            # url = "https://mercury.postlight.com/parser?url={}".format(
            #     canonical.encode('utf-8', errors='ignore'))
            yield scrapy.Request(
                url.decode(),
                callback=self.parse_item,
                errback=self.errback_request,
                meta=dict(
                    url_id=url_id,
                    date_captured=created_at,
                    date_published=date_published,
                    site_id=site_id,
                    canonical_url=canonical)#,
                )
                # headers={'x-api-key': self.api_key})

    def errback_request(self, failure):
        """Back call when error of the request.

        This function is called when exceptions are raise by middlewares.
        """
        request = failure.request
        try:
            self.session.query(Url).filter_by(id=request.meta['url_id'])\
                .update(dict(status_code=U_WP_ERROR_UNKNOWN))
            self.session.commit()
        except SQLAlchemyError as e:
            logger.error(e)
            self.session.rollback()

    def parse_item(self, response):
        """Parse the response into an ArticleItem."""
        status_code = None
        url_id = response.meta['url_id']
        item = ArticleItem(
            date_captured=response.meta['date_captured'],
            date_published=response.meta['date_published'],
            canonical_url=response.meta['canonical_url'],
            site_id=response.meta['site_id'],
            url_id=url_id)
        # load json data
        try:
            data = dict(html=response.text)
        except Exception as e:
            logger.warning('Error when loading response from webpaser: %s', e)
        # exceptions of the data
        if data is None:
            status_code = U_WP_ERROR_DATA_INVALID
        else:
            # fill item with data
            try:
                # First use Mercury
                try:
                    canonical_url = item['canonical_url']
                    logger.warning(canonical_url)
                    escaped_url = quote(canonical_url, safe='/:?=&')
                    if canonical_url is not None and canonical_url != "":
                        try:
                            mercury_parse = subprocess.check_output([
                                self.node_path, self.mercury_parser_installation_path,
                                escaped_url])
                            mercury_parse = demjson.decode(mercury_parse.decode('utf-8'))
                            if 'error' not in mercury_parse:
                                data['content'] = lxml.html.fromstring(
                                    html=mercury_parse['content']).text_content()
                                data['title'] = mercury_parse['title']
                                data['author'] = mercury_parse['author']
                                data['dek'] = mercury_parse['dek']
                                data['excerpt'] = mercury_parse['excerpt']
                                data['date_published'] = mercury_parse['date_published']
                                if data['content'] is None:
                                    raise Exception('No content found!')
                                if data['title'] is None:
                                    raise Exception('No title found!')
                            else:
                                logger.error('ConnectionTimedOut from Mercury script for url %s ', canonical_url)
                                raise Exception('Mercury timeout error. Try with Newspaper3k')
                        except subprocess.CalledProcessError as grepexc:
                            logger.error('exit code %s ', grepexc.returncode)
                            if grepexc.returncode != 0:
                                logger.error('Error while parsing with mercury. Try with Newspaper3k')
                                raise Exception('Mercury timeout error !')
                    else:
                        logger.error('URL is empty')
                except Exception as e:
                    logger.error('Error when parsing with Mercury: %s', e)
                    logger.info('Now parsing with Newspaper.')
                    try:
                        newspaper_article = npArticle(url='')
                        if data['html']:
                            newspaper_article.set_html(data['html'])
                            newspaper_article.config.fetch_images = False
                            newspaper_article.set_top_img(None)
                            newspaper_article.set_top_img("")
                        else:
                            raise Exception('Newspaper returned null content.')
                        newspaper_article.parse()
                        if data['content'] is None:
                            data['content'] = newspaper_article.text
                        if data['title'] is None:
                            data['title'] = newspaper_article.title
                        if data['date_published'] is None:
                            data['date_published'] = newspaper_article.publish_date
                        if data['excerpt'] is None:
                            data['excerpt'] = newspaper_article.meta_description
                        if data['author'] is None:
                            if newspaper_article.authors:
                                data['author'] = newspaper_article.authors[0]
                            else:
                                data['author'] = None
                    except Exception as e:
                        logger.warning('Error when using newspaper: %s', e)
                finally: # Fill with None if not exists
                    data.setdefault('content', None)
                    data.setdefault('title', None)
                    data.setdefault('dek', None)
                    data.setdefault('excerpt', None)
                    data.setdefault('author', None)
                    data.setdefault('date_published', None)
                item['title'] = data['title']
                content = data['content']
                item['content'] = content
                item['meta'] = dict(
                    dek=data['dek'],
                    excerpt=data['excerpt'],
                    author=data['author'])
                if 'date_published' not in item or item['date_published'] in (None, '', False):
                    item['date_published'] = data['date_published']
                if None in (item['title'], item['content']):
                    raise Exception('No proper content/title found')
            except Exception as e:
                logger.error('Error when parsing data from webparser, error is %s ', e)
                status_code = U_WP_ERROR_DATA_INVALID
        # No error happens, send to pipeline
        # remeber pipeline will also handle the url.status_code
        if status_code is None:
            status_code = U_WP_SUCCESS
            yield item
        else:
            # Update url status with error code
            try:
                self.session.query(Url).filter_by(id=url_id)\
                    .update(dict(status_code=status_code))
                self.session.commit()
            except SQLAlchemyError as e:
                logger.error(e)
                self.session.rollback()

    def close(self, reason):
        """Called when spider is closing."""
        logger.warning('%s closed with reason=%r', self.name, reason)
