# -*- coding: utf-8 -*-
"""This module provide different kinds of spiders for crawling processes.

There three kinds of crawling processes:
    (1) Fetch URL. Spiders are:
        a. FeedSpider (update),
        b. PageSpider(update),
        c. SitemapSpider(archive),
        d. PageTemplateSpider(archive),
        e. SiteSpider(archive)
    (2) Fetch HTML. Spider is HtmlSpider
    (3) Parse article. Spider is ArticleParserSpider

The intention of parameter `url_regex` is try to filter out non-article url.
However it was too hard to determine the pattern. Also we also collect URLs
from twitter, which is not filtered out. Thus `url_regex` is just a placeholder
right now, no function.

It takes very long time to crawl the entire site to collect all archieve
article. If the articles are list in a page template (most wordpress powered
site own this pattern), you can use PageTemplateSpider. This Spider is much
preciser (in terms of article) and faster.
"""

#
# written by Chengcheng Shao <sccotte@gmail.com>

from hoaxy.utils import list_cls_under_mod
import scrapy
import sys


def build_spiders_iter(msite, purpose):
    """Build spiders for a site.

    Parameters
    ----------
    msite : object
        ORM Site model instance.
    platform_id : int
        The id of a platform record in DB.
    purpose : {'update', 'archive'}
        Specify which kind of spiders to build, 'update' or 'archive'.

    Returns
    -------
    Generator
        A generator of built spiders for msite.
    """
    all_spiders = list_cls_under_mod(sys.modules[__name__],
                                     scrapy.spiders.Spider, 'name')
    domains = [msite.domain]
    domains += [ad.name for ad in msite.alternate_domains]
    rules = msite.article_rules[purpose]
    for rule in rules:
        yield dict(
            cls=all_spiders[rule['spider_name']],
            args=(domains,),
            kwargs=rule['spider_kwargs']
        )
