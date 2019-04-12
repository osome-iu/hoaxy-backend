#1. Why sites.yaml?
Domain lists, e.g., `domains_claim.txt`, only provides the very basic
information for site, `domain` and `site_type`. If you want to finer
control the site, e.g., how to crawl the site, you need the
`sites.yaml` file.

#2 How to write `sites.yaml`?
Of course, `sites.yaml` is a YAML file. You need the basic knowledge
of YAML format before editing. YAML is quite similar as JSON. The topest
hierarchy of the `sites.yaml` is a list object. And each element of this list is a site
entry.

Each site entry represents a site. Now let us go through each field of a
site entry.

## (1) Required fields. You must provide the following fields when edit a site entry.

  * `name`, string. The name of the site, often equals to its domain. 
  Example:

  ```yaml
  # name of snopes.com
  name: snopes.com
  ```
  * `domain`, string. The *primary* domain of the site. The term *primary* means that this domain shall not be redirected when visiting the site by it. You need to consider this issue when a site owns multiple domains.
  Example:

  ```yaml
  # domain of snopes.com
  domain: snopes.com
  ```
  * `site_type`, string. The type of the site. only three values are allowed:
    - 'claim', suspicious, e.g., bias, fake, conspiracy sites.
    - 'fact_checking', fact checking sites.
    - 'normal', normal main stream sites.
  Example:

  ```yaml
  # site_type of snopes.com
  site_type: fact_checking
  ```

## (2) Optional fields.

  * `base_url`, string. The home URL of the site. Default is inferred from its domain.
  Example:

  ```yaml
  # base_url of snopes.com
  base_url: http://www.snopes.com/
  ```
  * `site_tags`, list of `site_tag` object. Default is an empty list `[]`. The `site_tag` is a finer classification compared with `site_type`. A site can only have one `site_type`, but it can own multiple `site_tag`s. A `site_tag` object contains two attributes:
    - `name`, string. The name of tag.
    - `source`, string. Who mark the site as this tag.
  [Opensources](https://github.com/BigMcLargeHuge/opensources/blob/master/notCredible/notCredible.csv) provides a list of sites with tags.
  Example:

  ```yaml
  # site_tags of snopes.com marked by us
  site_tags:
  - `name`: fact_checking
    `source`: hoaxy.iuni.iu.edu
  ```
  * alternate_domains, list of `alternate_domain` object. Default is an empty list `[]`. Some site own multiple domains. Except the primary domain, other domains are treated as alternate domains. An `alternate_domain` contains two attributes:
    - `name`, string. The name of the alternate domain.
    - `is_alive`, bool. Whether this alternate domain is alive. This is an optional attributes. By default, `true`.
  Example:

  ```yaml
  # alternate domains of snopes.com
  alternate_domains: []
  ```
  * `is_alive`, bool. Whether this site is alive now. Default is `true`.
  Example:

  ```yaml
  # is_alive of snopes.com
  is_alive: true
  ```
  * `is_enabled`, bool. Whether this site is enabled. Default is `true`. If `false`, the twitter tracking process and URL crawling processes will ignore this site.
  Example:

  ```yaml
  # is_enabled of snopes.com
  is_enabled: true
  ```
  * `article_rules`, object. This is the most complicate object that specify how to crawl the site. This object contains the following attributes:
    - `url_regex`, string. The regular expression of URL we would like to collect. Right now it is a placeholder.
    - `update`, list. A list of update rules to specify how to fetch update of the site. Each update rule contains the following attributes:
      * `spider_name`, string. The name of the spider, should be consistent with hoaxy.crawl.spiders.
      * `spider_kwargs`, dict. The necessary parameters for building a spider instance. Often you do not need to provide `domains`, `session` and `platform_id` parameters. However, you must provide other required parameters.
    - `archive`, list. A list of archive rules to specify how to fetch archive of the site. The `archive` object is same as `update` object.
  Example:

  ```yaml
  # rules of how to crawl factcheck.org
  article_rules:
    # regular expression of url we like to collect
    # right now this field does not used, please ignore
    url_regex: ^http://www\.factcheck.org/20[0-2]\d/((0[1-9])|(1[0-2]))/[^/\s]+/?$
    # how to fetch the new articles update from factcheck.org
    # by default, page.spider, which crawls the home page of this site
    update:
      # here we use feed.spider as we have RSS feed URL,
      # see hoaxy.crawl.spiders
    - spider_name: feed.spider
      # the necessary parameters for building spider instance
      spider_kwargs:
        # here, we need the RSS feed URLs
        urls:
        - http://www.factcheck.org/feed/
        # and also who providse the RSS feed
        # normally the website itself, sometimes a third party, e.g. feedburner
        provider: self
    # how to fetch archive of factcheck.org
    # by default, site.page, which crawls the whole site.
    archive:
      # here we use page_template.spider, as factcheck.org use a page
      # template to list all of its posted articles
    - spider_name: page_template.spider
      spider_kwargs:
        # a list of xpaths to extract links (to find @href)
        # by default, a python tuple('/html/body',) is used
        # to fetch all links in this page
        # here, we use specified xpath expression
        # Note: please do not include /a/@href part
        href_xpaths:
        - //article//header/h2
        # page templates of factcheck.org
        # increasing by page number
        page_templates:
        - http://www.factcheck.org/page/{p_num}
    # factcheck.org also provides sitemap.xml to help us collect all
    # links in this site
    - spider_name: sitemap.spider
      spider_kwargs:
        # these URLs could be actual sitemap URL
        # OR they could be the entry of a list of sitemap.xml files
        # the spider will follow all XML links and
        # assuming these XML file are sitemaps and extract non-xml
        # links
        urls:
        - http://www.factcheck.org/sitemap.xml

  ```
## (3) Suggested ways to edit `aritcle_rules` for a site.
  * The RSS feed is a very good source to fetch update of a site. If you the site provide RSS, we suggest you use it.
  * If you do not care about the archives of the site (the old post), you can ignore `archive` rules. However if you do want the archives of the site. And the archive pages are visted by page numbers, e.g., 'http://example.com/p={p_num}', then you could provide the page template so that the spider could fast crawl all pages. Otherwise, the spider has to crawl the whole site by following each links, which is very time-consuming. The following is a list of spiders you may need to fetch URLs, and please check hoaxy.crawl.spiders module to see the details.

  ```python
  # feed.spider, used to fetch update via RSS feed.
  name = 'feed.spider'
  class = hoaxy.crawl.spiders.FeedSpider 
  """
  Main Parameters
  ---------------
  provider : string
      {'self' or the provider of this RSS}, if 'self', no namespace is needed
      when parsing RSS XML. Otherwise use this provider as namespace, e.g., 
      'feedburner'.
  urls : list
      A list of available RSS feed.
  """

  # page.spider, used to fetch update via limited URLs.
  name = 'page.spider'
  class = hoaxy.crawl.spiders.PageSpider
  """
  Main Parameters
  ---------------
  urls : list
    A list of URLs to get news update. Often they the home page of the site.
  """

  # page.template.spider, used to fetch archive via templated pages.
  name = 'page.template.spider'
  class = hoaxy.craw.spiders.PageTemplateSpider
  """
  Main Parameters
  ---------------
  pages_templates : list
    A list of URL templates. We support template of page number rotation only.
  """

  # site.spider, used to fetch archive via whole site crawling.
  name = 'site.spider'
  class = hoaxy.crawl.spider.SiteSpider
  """
  Main Parameters
  ---------------
  urls : list
    A list of URLs as the start URLs.

  """
```
