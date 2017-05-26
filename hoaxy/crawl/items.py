# -*- coding: utf-8 -*-
"""Scrapy pipline item classes.

This module provides scrapy item classes.

"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

import scrapy


class UrlItem(scrapy.Item):
    """Url entry, see Url table model."""
    id = scrapy.Field()
    raw = scrapy.Field()
    expanded = scrapy.Field()
    canonical = scrapy.Field()
    html = scrapy.Field()
    status_code = scrapy.Field()
    date_published = scrapy.Field()
    article_id = scrapy.Field()
    site_id = scrapy.Field()


class ArticleItem(scrapy.Item):
    """Article entry, see Article table model."""
    id = scrapy.Field()
    url_id = scrapy.Field()
    canonical_url = scrapy.Field()
    site_id = scrapy.Field()
    group_id = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()
    meta = scrapy.Field()
    date_published = scrapy.Field()
    date_captured = scrapy.Field()
