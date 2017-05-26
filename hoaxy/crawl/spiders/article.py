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
        api_key : string
            The API key to use the service.
        """
        self.session = session
        self.url_tuples = url_tuples
        self.api_key = kwargs.pop('api_key')
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
                self.session.query(Url).filter_by(id=url_id).update(dict(
                    article_id=marticle.id,
                    status_code=U_WP_SUCCESS
                ))
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
            url = "https://mercury.postlight.com/parser?url={}".format(
                canonical.encode('utf-8', errors='ignore'))
            yield scrapy.Request(
                url,
                callback=self.parse_item,
                errback=self.errback_request,
                meta=dict(url_id=url_id, date_captured=created_at,
                          date_published=date_published, site_id=site_id,
                          canonical_url=canonical),
                headers={'x-api-key': self.api_key})

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
        item = ArticleItem(date_captured=response.meta['date_captured'],
                           date_published=response.meta['date_published'],
                           canonical_url=response.meta['canonical_url'],
                           site_id=response.meta['site_id'],
                           url_id=url_id)
        # load json data
        try:
            data = json.loads(response.text)
        except Exception as e:
            data = None
            logger.warning('Error when loading response from webpaser: %s', e)
        # exceptions of the data
        if data is None or 'title' not in data or len(data['title']) == 0:
            status_code = U_WP_ERROR_DATA_INVALID
        else:
            # fill item with data
            try:
                item['title'] = data['title']
                content = data['content'] if data['content'] else None
                if content is not None:
                    content = lxml.html.fromstring(html=content).text_content()
                item['content'] = content
                item['meta'] = dict(dek=data['dek'],
                                    excerpt=data['excerpt'],
                                    author=data['author']
                                    )
                if item['date_published'] is None:
                    item['date_published'] = data['date_published']
            except Exception as e:
                logger.error('Error when parsing data from webparser %r: %s',
                        data, e)
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
