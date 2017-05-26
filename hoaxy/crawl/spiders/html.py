# -*- coding: utf-8 -*-
"""This module provides spiders to fetch HTML from URLs existed in database.
"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

from hoaxy.crawl.items import UrlItem
from hoaxy.database.models import U_DEFAULT
from hoaxy.database.models import U_HTML_ERROR_EXCLUDED_DOMAIN
from hoaxy.database.models import U_HTML_ERROR_HOME_URL
from hoaxy.database.models import U_HTML_ERROR_INVALID_HTML
from hoaxy.database.models import U_HTML_ERROR_INVALID_URL
from hoaxy.database.models import U_HTML_ERROR_NONHTTP
from hoaxy.database.models import U_HTML_ERROR_NO_SCRAPY_RESPONSE
from hoaxy.database.models import U_HTML_SUCCESS
from hoaxy.database.models import Url
from hoaxy.utils.url import belongs_to_domain
from hoaxy.utils.url import get_parsed_url
from hoaxy.utils.url import is_home_url
from scrapy.spidermiddlewares.httperror import HttpError
from sqlalchemy.exc import SQLAlchemyError
import logging
import scrapy

logger = logging.getLogger(__name__)


class HtmlSpider(scrapy.spiders.Spider):
    """This spider fetches HTML of URLs from database and update the
    the `status_code` of the URLs.

    Main steps of the processing is:
        (1) check whether host of URL in excluded_domains.
        (2) check whether host of URL is home URL.
        (2) crawling.
        (3) send to pipeline to update database.
    """
    name = 'urlhtml.spider'

    def __init__(self, url_tuples, *args, **kwargs):
        """Constructor of the HtmlSpider.

        Parameter:
        url_tuples : list
            A list of URL tuple (id, raw).
        session : object
            A instance of SQLAlchemy session.
        excluded_domains : list
            A list of domains (optional).
        """
        self.session = kwargs.pop('session', None)
        self.url_tuples = url_tuples
        self.excluded_domains = kwargs.pop('excluded_domains', ())
        super(HtmlSpider, self).__init__(*args, **kwargs)

    def errback_request(self, failure):
        """Back call when exceptions happens of the scrapy internal

        This function is called after download middlewares and spider
        middlewares.

        NOTE: if it is a HTTP error, the response must pass through all
        download middlewares.
        """
        request = failure.request
        item = UrlItem()
        item['id'] = request.meta['url_id']
        item['raw'] = request.meta['raw']
        if failure.check(HttpError):
            # these exceptions come from HttpError spider middleware
            # you can get the non-200 response
            response = failure.value.response
            item['status_code'] = response.status
            item['expanded'] = response.url
            logger.error('HTTP ERROR % r when fetching % r',
                         item['status_code'], item['raw'])
        else:
            item['expanded'] = item['raw']
            item['status_code'] = U_HTML_ERROR_NONHTTP
            logger.error('NON-HTTP error when fetching url %r: %s',
                         item['raw'], repr(failure))
        yield item

    def start_requests(self):
        for url_id, raw in self.url_tuples:
            purl = get_parsed_url(raw)
            status_code = None
            if purl is None or purl.hostname is None:
                status_code = U_HTML_ERROR_INVALID_URL
                logger.debug('Invalide url %r', raw)
            elif is_home_url(purl.path) is True:
                status_code = U_HTML_ERROR_HOME_URL
                logger.debug('Ignore home kind url %r', raw)
            elif belongs_to_domain(purl.hostname,
                                   self.excluded_domains) is not None:
                status_code = U_HTML_ERROR_EXCLUDED_DOMAIN
                logger.debug('Ignore excluded domain %r', raw)
            else:
                try:
                    yield scrapy.Request(raw,
                                         callback=self.parse,
                                         meta=dict(url_id=url_id,
                                                   raw=raw),
                                         errback=self.errback_request)
                except Exception as e:
                    logger.error('Error when sending request %r: %s', raw, e)
                    status_code = U_HTML_ERROR_INVALID_URL
            # when error happends, update url status_code
            if status_code is not None:
                try:
                    self.session.query(Url).filter_by(id=url_id)\
                        .update(dict(status_code=status_code),
                                synchronize_session=False)
                    self.session.commit()
                except SQLAlchemyError as e:
                    logger.error('Error when update url status_code: %s', e)
                    self.session.rollback()

    def parse(self, response):
        # handle only HTTP=200
        item = UrlItem()
        item['id'] = response.meta['url_id']
        item['raw'] = response.meta['raw']
        item['expanded'] = response.url
        try:
            text = response.body
            code = response.encoding
            html = unicode(text.decode(code, 'ignore'))\
                .encode('utf-8', 'ignore')
            item['html'] = html
            item['status_code'] = U_HTML_SUCCESS
        except Exception as e:
            logger.error('Invalidate html docs: %s, %s', item['raw'], e)
            item['status_code'] = U_HTML_ERROR_INVALID_HTML
        yield item

    def close(self, reason):
        """Called when closing this spider.

        When closing spider, if the reason is 'finished' (meaning
        successfully run the crawling process), we set `status_code` of
        these response non-received URLs as U_HTML_ERROR_NO_SCRAPY_RESPONSE.
        """
        if reason == 'finished':
            logger.warning("""Update unreceived record when closing spider \
with reason='finished""")
            try:
                self.session.query(Url)\
                    .filter_by(status_code=U_DEFAULT)\
                    .filter(Url.id.in_(tuple(url_id for url_id,
                                             raw in self.url_tuples)))\
                    .update(dict(status_code=U_HTML_ERROR_NO_SCRAPY_RESPONSE),
                            synchronize_session=False)
                self.session.commit()
            except SQLAlchemyError:
                raise
        logger.warning('%s closed with reason=%r', self.name, reason)
