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
    (6) save to table article where 'site_id is not null and
        status_code=U_HTML_SUCCESS'
3. Webparser to get parsed article:
    (1) For record in article table, do article parsing

There exists one pipeline for each phase.

"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

import logging

from scrapy.exceptions import DropItem
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.sql import func

from hoaxy.crawl.items import ArticleItem
from hoaxy.database.functions import get_or_create_murl, get_site_tuples
from hoaxy.database.models import (MAX_URL_LEN,
                                   U_HTML_ERROR_EXCLUDED_DOMAIN,
                                   U_HTML_ERROR_INVALID_URL, U_HTML_SUCCESS,
                                   Article, Url,)
from hoaxy.utils.url import (belongs_to_domain, belongs_to_site, canonicalize,
                             get_parsed_url,)

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
    """This Pipeline is used to (1) update status_code of existed url records,
    and (2) insert a new article with html into article if not exist.
    """

    def open_spider(self, spider):
        """Get sites when opening the spider."""
        self.site_tuples = get_site_tuples(spider.session)

    def get_or_create_marticle(self, session, article_data):
        mquery = session.query(Article).filter_by(
            canonical_url=article_data['canonical_url'])
        marticle = mquery.one_or_none()
        if marticle is None:
            session.add(Article(**article_data))
        else:
            if marticle.date_captured > article_data['date_captured']:
                marticle.date_captured = article_data['date_captured']
        try:
            session.commit()
            return marticle
        except IntegrityError as e:
            logger.error('Concurrent error: %s', e)
            return mquery.one()

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
        # if status_code==U_HTML_SUCCESS AND site_id IS NOT NULL
        # create a new article record
        if item['status_code'] == U_HTML_SUCCESS\
                and item['site_id'] is not None:
            # remove potential NUL byte \x00 in the HTML
            html = item.pop('html')
            article_data = ArticleItem()
            article_data['html'] = html
            article_data['site_id'] = item['site_id']
            article_data['date_captured'] = item['created_at']
            # created or update article table
            try:
                marticle = self.get_or_create_marticle(spider.session,
                                                       article_data)
                item['article_id'] = marticle.id
            except SQLAlchemyError as e:
                logger.error(e)
                spider.session.rollback()
                raise DropItem('Fail to update database of url: %s', item)
        try:
            # update database of url table
            spider.session.query(Url).filter_by(id=item['id'])\
                .update(dict(item))
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
        article_id = item.pop('id')
        item['group_id'] = self.get_or_next_group_id(
            spider.session, item['title'], item['site_id'])
        # update changes
        spider.session.query(Article).filter_by(id=article_id).update(item)
        try:
            spider.session.commit()
            return item
        except SQLAlchemyError as e:
            logger.error('Error when updating article: %s', e)
            spider.session.rollback()
            raise DropItem()
