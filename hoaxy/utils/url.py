# -*- coding: utf-8 -*-
"""Url utilities."""
#
# written by Chengcheng Shao <sccotte@gmail.com>

from hoaxy import CONF
from urllib.parse import urlparse
from w3lib.url import canonicalize_url
from w3lib.url import url_query_cleaner
import logging
import requests

logger = logging.getLogger(__name__)
if CONF['crawl']['scrapy'].get('USER_AGENT') is not None:
    HEADERS = {'user-agent': CONF['crawl']['scrapy']['USER_AGENT']}
else:
    HEADERS = dict()
HTTP_TIMEOUT = 30  # HTTP REQUEST TIMEOUT, IN SECONDS


def canonicalize(url,
                 remove_parameters=('utm_medium', 'utm_source', 'utm_campaign',
                                    'utm_term', 'utm_content')):
    """Canonicalize URL."""
    try:
        curl = url_query_cleaner(
            canonicalize_url(
                url, keep_blank_values=False, keep_fragments=False),
            parameterlist=remove_parameters,
            remove=True)
        return canonicalize_url(
            curl, keep_blank_values=False, keep_fragments=False)
    except Exception as e:
        logger.warning('Fail to canonicalize url %r: %s', url, e)
        return None


def get_parsed_url(url):
    """Parse the URL and return host name of this URL."""
    try:
        return urlparse(url)
    except Exception as e:
        logger.error('Fail to parse url %r: %s', url, e)
        return None


def is_home_url(path):
    """Test whether the path is home URL."""
    if path is None or len(path) == 0:
        return True
    else:
        return False


def belongs_to_domain(host, domains):
    """If host belongs one of the domains, return the first one, else None."""
    for d in domains:
        if host == d or host.endswith('.%s' % d):
            return d
    return None


def belongs_to_site(host, site_tuples):
    """If host belongs one of the site domains, return the site_id."""
    for site_id, d in site_tuples:
        if host == d or host.endswith('.%s' % d):
            return site_id
    return None


def belongs_to_msite(host, msites):
    """If host belongs one of the msite domains (including alternate domains),
    return msite."""
    for ms in msites:
        if host == ms.domain or host.endswith('.%s' % ms.domain):
            return ms
        for d in msites.alternate_domains:
            if host == d or host.endswith('.%s' % d):
                return ms
    return None


def infer_base_url(domain):
    """Fetch the base URL of a domain by sending HTTP HEAD request."""
    try:
        r = requests.head(
            'http://' + domain,
            allow_redirects=True,
            headers=HEADERS,
            timeout=HTTP_TIMEOUT)
        base_url = r.url
        if not base_url.endswith('/'):
            base_url = base_url + '/'
    except Exception as e:
        logger.error(e)
        base_url = None
    if base_url is None:
        try:
            r = requests.head(
                'https://' + domain,
                allow_redirects=True,
                headers=HEADERS,
                timeout=HTTP_TIMEOUT)
            base_url = r.url
            if not base_url.endswith('/'):
                base_url = base_url + '/'
        except Exception as e:
            logger.error(e)
            base_url = None
    return base_url


def owns_url(domain, url):
    """Test whether a `domain` belongs to the `url`."""
    try:
        o = urlparse(url)
        h = o.hostname
        if h == domain or h.endswith('.%s' % domain):
            return True
        else:
            return False
    except Exception as e:
        logger.error(e)
        return False
