# -*- coding: utf-8 -*-
"""Scrapy pipelines.

There are three phrase when processing URLs.
1. Collect URLs from spiders OR twitter streaming API, at this moment
   except INSERT them as raw, do nothing on these URLs, so the status_code=0
2. For URL from 1., fetch the html page:
    (1) check whether raw in exclude domains, if so,
        set status_code=U_HTML_FAILED_EXCLUDED_DOMAIN
    (2) do crawling process, check HTTP response
    (3) if failed, set status_code and set expanded=raw
    (4) if success, set status_code and set expanded=response.url
    (5) determine site_id
3. Webparser to get parsed article:
    (1) only select 'site_id is not null and status_code=U_HTML_SUCCESS'

There exists one pipeline for each phase.

"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

from hoaxy.database.functions import get_or_create_murl
from hoaxy.database.functions import get_site_tuples
from hoaxy.database.models import MAX_URL_LEN
from hoaxy.database.models import U_HTML_ERROR_EXCLUDED_DOMAIN
from hoaxy.database.models import U_HTML_ERROR_INVALID_URL
from hoaxy.database.models import U_WP_SUCCESS
from hoaxy.database.models import Url, Article
from hoaxy.utils.url import belongs_to_domain
from hoaxy.utils.url import belongs_to_site
from hoaxy.utils.url import canonicalize
from hoaxy.utils.url import get_parsed_url
from scrapy.exceptions import DropItem
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import load_only
from sqlalchemy.sql import func
import logging

logger = logging.getLogger(__name__)


class UrlPipeline(object):
    """This Pipeline is used to insert item into url table."""

    def open_spider(self, spider):
        """Get sites when opening the spider."""
        self.site_tuples = get_site_tuples(spider.session)

    def process_item(self, item, spider):
        """Main function that process URL item (first phase)."""
        # validate URL length
        if len(item['raw']) > MAX_URL_LEN:
            item['raw'] = item['raw'][:MAX_URL_LEN]
            logger.error('Raw URL too long, trucate it! %r', item['raw'])
        # parse raw URL
        purl = get_parsed_url(item['raw'])
        if purl is None or purl.hostname is None:
            raise DropItem('Invalide URL')
        site_id = belongs_to_site(purl.hostname, self.site_tuples)
        if site_id is None:
            raise DropItem('Offsite domain: %s', item)
        item['site_id'] = site_id
        # insert URL into table
        try:
            get_or_create_murl(spider.session, item, spider.platform_id)
        except SQLAlchemyError as e:
            logger.error(e)
            spider.session.rollback()
            raise DropItem('Fail to insert database of url: %s', item)
        return item


class HtmlPipeline(object):
    """This Pipeline is used to update the html document and status_code
    of an existed url record in the DB."""

    def open_spider(self, spider):
        """Get sites when opening the spider."""
        self.site_tuples = get_site_tuples(spider.session)

    def process_item(self, item, spider):
        """Main function that process URL item (second phase)."""
        # canonicalize expanded URL without considering the status_code
        # because scrapy crawling not ganrantee the success
        # we still try to canonicalize the URL
        if len(item['expanded']) > MAX_URL_LEN:
            item['expanded'] = item['expanded'][:MAX_URL_LEN]
            logger.error('Expanded URL too long, trucate it! %r', item['raw'])
        item['canonical'] = canonicalize(item['expanded'])
        if item['canonical'] is None:
            item['status_code'] = U_HTML_ERROR_INVALID_URL

        # if url could be canonicalized and if site_id is not determined
        # we infer it from the expanded url
        if item['status_code'] != U_HTML_ERROR_INVALID_URL\
                and item.get('site_id', None) is None:
            purl = get_parsed_url(item['expanded'])
            if purl is not None and purl.hostname is not None:
                if belongs_to_domain(purl.hostname, spider.excluded_domains)\
                        is not None:
                    item['status_code'] = U_HTML_ERROR_EXCLUDED_DOMAIN
                else:
                    item['site_id'] = belongs_to_site(purl.hostname,
                                                      self.site_tuples)
            else:
                item['status_code'] = U_HTML_ERROR_INVALID_URL
        # remove potential NUL byte \x00 in the HTML
        if 'html' in item:
            item['html'] = item['html'].replace(b'\x00', b'')
        try:
            # update database of url table
            spider.session.query(Url).filter_by(id=item['id'])\
                .update(dict(item), synchronize_session=False)
            spider.session.commit()
            logger.debug('Fetched html of url %r with status %i', item['raw'],
                         item['status_code'])
        except SQLAlchemyError as e:
            logger.error(e)
            spider.session.rollback()
            raise DropItem('Fail to update database of url: %s', item)
        return item


class ArticlePipeline(object):
    """The class is used to save parsed article into table."""

    def get_max_group_id(self, session):
        """Return the maximumn group_id of table `article`.

        Parameters
        ----------
        session : object
            A SQLAlchemy Session instance.

        Returns
        -------
        int
            The maximum group_id of table `article`.

        """
        group_id = session.query(func.max(Article.group_id)).scalar()
        return group_id if group_id is not None else 0

    def get_or_next_group_id(self, session, title, site_id):
        """Get the next group_id when trying to insert a new article.

        Parameters
        ----------
        session : object
            A instance of SQLAlchemy Session.
        title : string
            The title of an article.
        site_id : int
            The site_id of an article.

        Returns
        -------
        int
            The next group_id.
        """
        group_id = session.query(Article.group_id).filter_by(
            title=title, site_id=site_id).limit(1).scalar()
        return group_id if group_id is not None \
            else self.get_max_group_id(session) + 1

    def process_item(self, item, spider):
        """Main function that process Article item (third phase)."""
        url_id = item.pop('url_id')
        marticle = spider.session.query(Article)\
            .filter_by(canonical_url=item['canonical_url'])\
            .options(load_only('id', 'date_published', 'date_captured'))\
            .one_or_none()
        # marticle exists
        # update datetime of this article
        if marticle:
            # marticle.date_published is None
            if marticle.date_published is None:
                marticle.date_published = item['date_published']
            # marticle.date_captured > article['date_captured']
            if marticle.date_captured > item['date_captured']:
                marticle.date_captured = item['date_captured']
        # new article
        else:
            if item['site_id'] is not None:
                item['group_id'] = self.get_or_next_group_id(
                    spider.session, item['title'], item['site_id'])
            # create this article
            marticle = Article(**item)
            spider.session.add(marticle)
        # commit changes
        try:
            spider.session.commit()
        except SQLAlchemyError as e:
            logger.error('Error when inserting article: %s', e)
            spider.session.rollback()
            raise DropItem()
        try:
            # finally update url status and article_id
            spider.session.query(Url).filter_by(id=url_id)\
                .update(dict(status_code=U_WP_SUCCESS,
                             article_id=marticle.id))
            spider.session.commit()
        except SQLAlchemyError as e:
            logger.error('Error when update url: %s', e)
            spider.session.rollback()
            raise DropItem()
        item['url_id'] = url_id
        item['id'] = marticle.id
        return item
