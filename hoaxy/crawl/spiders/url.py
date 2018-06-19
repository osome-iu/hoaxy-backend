# -*- coding: utf-8 -*-
"""This module provides spiders to fetch URL from news websites.
"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

import copy
import logging
import pprint
import urlparse
from datetime import datetime

import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule

from hoaxy.crawl.items import UrlItem
from hoaxy.database.models import U_HTML_SUCCESS
from hoaxy.utils.dt import utc_from_str
from hoaxy.utils.url import canonicalize

logger = logging.getLogger(__name__)


class FeedSpider(scrapy.spiders.XMLFeedSpider):
    """FeedSpider is used to crawl updates of news websits by RSS feed.

    There are two ways when a website provides its XML website: one is
    provided by self, the other is provided by third party, e.g., feedburner.
    """
    name = 'feed.spider'

    def __init__(self, domains, urls, *args, **kwargs):
        """Constructor for FeedSpider.

        Parameters
        ----------
        domains : list
            A list of domains for the site.
        urls : list
            A list of feed URLs of the site.
        provider : string
            The provider of RSS feed.
        url_regex : string
            URL pattern regular expression.

        If you use this spider to store item into database, additional
        keywords are required:

        platform_id : int
            The id of a platform instance.
        session : object
            An instance of SQLAlchemy session.

        Other keywords are used to specify how to parse the XML, see
        http://doc.scrapy.org/en/latest/topics/spiders.html#scrapy.spiders\
        .XMLFeedSpider.
        """
        self.platform_id = kwargs.pop('platform_id', None)
        self.session = kwargs.pop('session', None)
        self.url_regex = kwargs.pop('url_regex', None)
        self.provider = kwargs.pop('provider', 'self')
        self.iterator = kwargs.pop('iterator', 'iternodes')
        self.itertag = kwargs.pop('iterator', 'item')
        self.allowed_domains = domains
        self.start_urls = urls
        super(FeedSpider, self).__init__(*args, **kwargs)

    def parse_node(self, response, node):
        """Parse response into UrlItem."""
        item = UrlItem()
        if self.provider == 'self':
            link = node.xpath('link/text()').extract_first()
        elif self.provider == 'feedburner':
            link = node.xpath(
                '*[local-name()="origLink"]/text()').extract_first()
        else:
            logger.error('Unrecognized feed provider %r', self.provider)
            return
        date_published = node.xpath('pubDate/text()').extract_first()
        if link is not None:
            item['raw'] = link.strip()
            item['date_published'] = utc_from_str(date_published)
            yield item
        else:
            logger.error('Unexpected item: (%s, %s) from %r', link,
                         date_published, response.url)
            return

    def close(self, reason):
        logger.info('%s for %s closed with reason=%r', self.name,
                    self.allowed_domains, reason)


class SitemapSpider(scrapy.spiders.Spider):
    """SitemapSpider is used to extract URLS from sitemap.xml."""
    name = 'sitemap.spider'

    def __init__(self, domains, urls, *args, **kwargs):
        """Constructor for SitemapSpider.

        Parameters
        ----------
        domains : list
            A list of domains for the site.
        urls : list
            A list of sitemap URLs of the site.
        url_regex : string
            URL pattern regular expression.

        If you use this spider to store item into database, additional
        keywords are required:

        platform_id : int
            The id of a platform instance.
        session : object
            An instance of SQLAlchemy session.
        """
        self.session = kwargs.pop('session', None)
        self.platform_id = kwargs.pop('platform_id', None)
        self.url_regex = kwargs.pop('url_regex', None)
        self.allowed_domains = domains
        self.start_urls = urls
        super(SitemapSpider, self).__init__(*args, **kwargs)

    def parse(self, response):
        for link in response.xpath('//*[local-name()="loc"]/text()').extract():
            item = UrlItem()
            link = link.strip()
            # check if this link is another sitemap link
            if link.endswith('.xml'):
                yield scrapy.Request(link, callback=self.parse)
            else:
                item['raw'] = link
                yield item

    def close(self, reason):
        logger.info('%s for %s closed with reason=%r', self.name,
                    self.allowed_domains, reason)


class PageSpider(scrapy.spiders.Spider):
    """PageSpider is a simple spider that extract all in-site links
    without following.
    """
    name = 'page.spider'

    def __init__(self, domains, urls, *args, **kwargs):
        """Constructor for PageSpider.

        Parameters
        ----------
        domains : list
            A list of domains for the site.
        urls : list
            A list of URLs of the site.
        href_xpaths : list
            A list of XPATH expression indicating the ancestors of `<a>`
            element.
        url_regex : string
            URL pattern regular expression.

        If you use this spider to store item into database, additional
        keywords are required:

        platform_id : int
            The id of a platform instance.
        session : object
            An instance of SQLAlchemy session.
        """
        self.session = kwargs.pop('session', None)
        self.platform_id = kwargs.pop('platform_id', None)
        self.href_xpaths = kwargs.pop('href_xpaths', ())
        self.url_regex = kwargs.pop('url_regex', None)
        self.start_urls = urls
        self.allowed_domains = domains
        self.link_extractor = LinkExtractor(
            allow_domains=self.allowed_domains,
            restrict_xpaths=self.href_xpaths,
            unique=True)
        super(PageSpider, self).__init__(*args, **kwargs)

    def parse(self, response):
        for link in self.link_extractor.extract_links(response):
            item = UrlItem()
            item['raw'] = link.url
            yield item

    def close(self, reason):
        logger.info('%s for %s closed with reason=%r', self.name,
                    self.allowed_domains, reason)


class SiteSpider(scrapy.spiders.CrawlSpider):
    """SiteSpider is used to crawl a whole site."""
    name = 'site.spider'

    def __init__(self, domains, urls, *args, **kwargs):
        """Constructor for SiteSpider.

        Parameters
        ----------
        domains : list
            A list of domains for the site.
        urls : list
            A list of sitemap URLS of the site.
        href_xpaths : list
            A list of XPATH expression indicating the ancestors of `<a>`
            element.
        url_regex : string
            URL pattern regular expression.

        If you use this spider to store item into database, additional
        keywords are required:

        platform_id : int
            The id of a platform instance.
        session : object
            An instance of SQLAlchemy session.
        """
        self.session = kwargs.pop('session', None)
        self.platform_id = kwargs.pop('platform_id', None)
        self.url_regex = kwargs.pop('url_regex', None)
        self.href_xpaths = kwargs.pop('href_xpaths', ())
        self.start_urls = urls
        self.allowed_domains = domains
        self.rules = (Rule(
            LinkExtractor(
                allow_domains=self.allowed_domains,
                restrict_xpaths=self.href_xpaths,
                unique=True),
            callback="parse_item",
            follow=True), )

        super(SiteSpider, self).__init__(*args, **kwargs)

    def parse_item(self, response):
        item = UrlItem()
        item['raw'] = response.url
        item['expanded'] = response.url
        item['canonical'] = canonicalize(item['expanded'])
        # set the created_at as current utc timestamp
        item['created_at'] = datetime.utcnow()
        try:
            text = response.body
            code = response.encoding
            html = unicode(text.decode(code, 'ignore'))\
                .encode('utf-8', 'ignore')
            html = html.replace(b'\x00', '')
            item['html'] = html
            item['status_code'] = U_HTML_SUCCESS
            yield item
        except Exception as e:
            logger.error('Invalidate html docs: %s, %s, %s', item['raw'], e,
                         text)
            # does not collect this item, if exception happens

    def close(self, reason):
        """Called when closing spider."""
        logger.info('%s for %s closed with reason=%r', self.name,
                    self.allowed_domains, reason)


class PageTemplateSpider(scrapy.spiders.Spider):
    """
    FormatPageSpider is used to collect article urls from page templates.

    `domain`, str, domain of the website
    `f_kw_meta`, dict, specify how to format page_url from page
    templates
    `pages`, list of str, pages templates
    `href_xpath`, str, xpath to retrieve href attribute of article urls
    """

    name = 'page_template.spider'

    def __init__(self, domains, page_templates, *args, **kwargs):
        """Constructor for PageTemplateSpider.

        Parameters
        ----------
        domains : list
            A list of domains for the site.
        page_templates : list
            A list of page templates, each of them represents a list of URLs
            that have same pattern. Currently we only support page templates
            that navigate site by rotating the page number, e.g., if the
            visting of site 'example.com' looks like 'http://example.com/\
            page_id=3', then the template is look like 'http://example.com/\
            page_id={p_num}.
        p_kw : dict
            A dict that specify the parameters to format the page_template.
            Keys are:
                start : int
                    The start page number. Default is 1.
                max_next_tries : int
                    The maximum tries when request fails. Default is 3.
        url_regex : string
            URL pattern regular expression.

        If you use this spider to store item into database, additional
        keywords are required:

        platform_id : int
            The id of a platform instance.
        session : object
            An instance of SQLAlchemy session.
        """
        self.session = kwargs.pop('session', None)
        self.platform_id = kwargs.pop('platform_id', None)
        self.href_xpaths = kwargs.pop('href_xpaths', ())
        self.url_regex = kwargs.pop('url_regex', None)
        self.allowed_domains = domains
        self.page_templates = page_templates
        self.p_kw = dict(start=1, max_next_tries=3)
        super(PageTemplateSpider, self).__init__(*args, **kwargs)

    def start_requests(self):
        """This function generates the initial request of ArchiveSpider.

        See 'http://doc.scrapy.org/en/latest/topics/spiders.html#\
        scrapy.spiders.Spider.start_requests'.

        The most import part of the function is to set a request meta,
        'archive_meta', according to its site 'archive_rules'. The meta would
        be used to parse article URLs from response and generate next request!
        """
        for page in self.page_templates:
            url = page.format(p_num=self.p_kw['start'])
            meta = dict(
                archive_meta=dict(
                    last_urls=dict(),
                    p_num=self.p_kw['start'],
                    next_tries=0,
                    max_next_tries=self.p_kw['max_next_tries'],
                    page=page))
            logger.debug('Page format meta info:\n%s', pprint.pformat(meta))
            yield scrapy.Request(url, callback=self.parse, meta=meta)

    def errback_request(self, failure):
        """When failure occurs, when make new try unless reaching
        max_next_tries."""
        request = failure.request
        meta = copy.copy(request.meta['archive_meta'])
        meta['p_num'] += 1
        meta['next_tries'] += 1
        if meta['next_tries'] < meta['max_next_tries']:
            url = meta['page'].format(p_num=meta['p_num'])
            meta = dict(archive_meta=meta)
            logger.debug('Page format meta info:\n%s', pprint.pformat(meta))
            yield scrapy.Request(
                url,
                callback=self.parse,
                errback=self.errback_request,
                meta=meta)
        else:
            logger.debug('Reach max next tries! Stop tring next page!')

    def parse(self, response):
        """Parse response into UrlItem."""
        found_new_url = False
        meta = copy.copy(response.meta['archive_meta'])
        del response.meta['archive_meta']
        urls = set()
        if len(self.href_xpaths) == 0:
            self.href_xpaths = ('/html/body', )
        for xp in self.href_xpaths:
            for href in response.xpath(xp).xpath('.//a/@href').extract():
                url = urlparse.urljoin(response.url, href)
                urls.add(url)
                if url not in meta['last_urls']:
                    found_new_url = True
                    item = UrlItem()
                    item['raw'] = urlparse.urljoin(response.url, url.strip())
                    yield item
        meta['last_urls'] = urls
        meta['p_num'] += 1
        if found_new_url is False:
            meta['next_tries'] += 1
        else:
            meta['next_tries'] = 0
        if meta['next_tries'] < meta['max_next_tries']:
            url = meta['page'].format(p_num=meta['p_num'])
            meta = dict(archive_meta=meta)
            logger.debug('Page format meta info:\n%s', pprint.pformat(meta))
            yield scrapy.Request(
                url,
                callback=self.parse,
                errback=self.errback_request,
                meta=meta)

    def close(self, reason):
        """Called when closing spider."""
        logger.info('%s for %s closed with reason=%r', self.name,
                    self.allowed_domains, reason)
