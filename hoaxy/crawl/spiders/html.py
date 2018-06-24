# -*- coding: utf-8 -*-
"""This module provides spiders to fetch HTML from URLs existed in database.
"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

import logging

import scrapy
from scrapy.spidermiddlewares.httperror import HttpError
from sqlalchemy.exc import SQLAlchemyError

from hoaxy.crawl.items import UrlItem
from hoaxy.database.models import (
    U_DEFAULT,
    U_HTML_ERROR_EXCLUDED_DOMAIN,
    U_HTML_ERROR_HOME_URL,
    U_HTML_ERROR_INVALID_HTML,
    U_HTML_ERROR_INVALID_URL,
    U_HTML_ERROR_DROPPED,
    U_HTML_ERROR_NONHTTP,
    U_HTML_SUCCESS,
    Url,
)
from hoaxy.utils.url import belongs_to_domain, get_parsed_url, is_home_url

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

    def __init__(self, nt_urls, *args, **kwargs):
        """Constructor of the HtmlSpider.

        Parameter:
        nt_urls : list
            A list of namedtuples, hoaxy.crawling.items.NTUrl
            (id, raw, created_at).
        session : object
            A instance of SQLAlchemy session.
        excluded_domains : list
            A list of domains (optional).
        """
        self.session = kwargs.pop('session', None)
        self.nt_urls = nt_urls
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
        nt_url = request.meta['nt_url']
        item['id'] = nt_url.id
        item['raw'] = nt_url.raw
        item['created_at'] = nt_url.created_at
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
        for nt_url in self.nt_urls:
            purl = get_parsed_url(nt_url.raw)
            status_code = None
            if purl is None or purl.hostname is None:
                status_code = U_HTML_ERROR_INVALID_URL
                logger.debug('Invalide url %r', nt_url.raw)
            elif is_home_url(purl.path) is True:
                status_code = U_HTML_ERROR_HOME_URL
                logger.debug('Ignore home kind url %r', nt_url.raw)
            elif belongs_to_domain(purl.hostname,
                                   self.excluded_domains) is not None:
                status_code = U_HTML_ERROR_EXCLUDED_DOMAIN
                logger.debug('Ignore excluded domain %r', nt_url.raw)
            else:
                try:
                    yield scrapy.Request(
                        nt_url.raw,
                        callback=self.parse,
                        meta=dict(nt_url=nt_url),
                        errback=self.errback_request)
                except Exception as e:
                    logger.error('Error when sending request %r: %s',
                                 nt_url.raw, e)
                    status_code = U_HTML_ERROR_INVALID_URL
            # when error happends, update url status_code
            if status_code is not None:
                try:
                    self.session.query(Url).filter_by(id=nt_url.id)\
                        .update(dict(status_code=status_code),
                                synchronize_session=False)
                    self.session.commit()
                except SQLAlchemyError as e:
                    logger.error('Error when update url status_code: %s', e)
                    self.session.rollback()

    def parse(self, response):
        # handle only HTTP=200
        item = UrlItem()
        nt_url = response.meta['nt_url']
        item['id'] = nt_url.id
        item['raw'] = nt_url.raw
        item['created_at'] = nt_url.created_at
        item['expanded'] = response.url
        try:
            text = response.body
            code = response.encoding
            html = unicode(text.decode(code, 'ignore'))\
                .encode('utf-8', 'ignore')
            html = html.replace(b'\x00', '')
            item['html'] = html
            item['status_code'] = U_HTML_SUCCESS
        except Exception as e:
            logger.error('Invalidate html docs: %s, %s, %s', item['raw'], e,
                         text.decode('utf-8', 'ignore'))
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
                    .filter(Url.id.in_([x.id for x in self.nt_urls]))\
                    .update(dict(status_code=U_HTML_ERROR_DROPPED),
                            synchronize_session=False)
                self.session.commit()
            except SQLAlchemyError as e:
                logger.error(e)
                self.session.rollback()
        logger.warning('%s closed with reason=%r', self.name, reason)
