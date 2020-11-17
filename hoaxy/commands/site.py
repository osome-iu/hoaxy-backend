# -*- coding: utf-8 -*-
"""Hoaxy subcommand SiteCmd implementation.

This commands provides site management functions.
"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

from hoaxy import HOAXY_HOME
from hoaxy.commands import HoaxyCommand
from hoaxy.crawl.spiders.url import FeedSpider, PageSpider, SiteSpider
from hoaxy.database import Session
from hoaxy.database.functions import get_msites
from hoaxy.database.functions import get_or_create_m
from hoaxy.database.functions import get_or_create_msite
from hoaxy.database.functions import qquery_msite
from hoaxy.database.functions import get_site_tuples
from hoaxy.database.models import Site, SiteTag, AlternateDomain
from hoaxy.utils.log import configure_logging
from hoaxy.utils.url import infer_base_url
from hoaxy.utils.url import owns_url
from os.path import join
from ruamel.yaml.comments import CommentedMap
import logging
import os
import pprint
import re
import ruamel.yaml
import sys
import yaml

logger = logging.getLogger(__name__)
# regular expression to match domain
DOMAIN_RE = re.compile('^([a-z0-9]+(-[a-z0-9]+)*\.)+[a-z]{2,}$')
REQ_FIELDS = ('name', 'domain', 'site_type')


def fill_rules(site):
    """Fill the article_rules field of a site.

    Parameters
    ----------
    site : dict
        A site object in dict form.
    """
    if 'article_rules' not in site:
        site['article_rules'] = dict(
            url_regex=None,
            update=[
                dict(
                    spider_name=PageSpider.name,
                    spider_kwargs=dict(urls=[site['base_url']]))
            ],
            archive=[
                dict(
                    spider_name=SiteSpider.name,
                    spider_kwargs=dict(urls=[site['base_url']]))
            ])
    else:
        if 'update' not in site['article_rules']:
            site['article_rules']['update'] = [
                dict(
                    spider_name=FeedSpider.name,
                    spider_kwargs=dict(urls=[site['base_url']]))
            ]
        if 'archive' not in site['article_rules']:
            site['article_rules']['archive'] = [
                dict(
                    spider_name=SiteSpider.name,
                    spider_kwargs=dict(urls=[site['base_url']]))
            ]
        if 'url_regex' not in site['article_rules']:
            site['url_regex'] = None


def is_comment_line(line):
    """Test whether this line is a blank or comment line.
    """
    line = line.strip()
    if line.startswith('#') or len(line) == 0:
        return True
    else:
        return False


def parse_domain(line, site_type):
    """Validate and parse the domain represented in the line.

    Parameters
    ----------
    line : string
    site_type : {'claim', 'fact_checking'}

    Returns
    -------
    tuple
        (site, status)
    """
    d = line.lower().strip()
    if DOMAIN_RE.match(d) is None:
        return (None, 'invalid')
    if d.startswith('www.'):
        d = d[4:]
    if len(d) <= 3:
        return (None, 'invalid')
    base_url = infer_base_url(d)
    if base_url is None:
        site = dict(
            name=d,
            domain=d,
            base_url='http://' + d + '/',
            site_type=site_type,
            is_alive=False)
        fill_rules(site)
        return (site, 'inactive')
    elif owns_url(d, base_url):
        site = dict(name=d, domain=d, base_url=base_url, site_type=site_type)
        fill_rules(site)
        return (site, 'ok')
    else:
        site = dict(name=d, domain=d, base_url=base_url, site_type=site_type)
        fill_rules(site)
        return (site, 'redirected')


def parse_site(site):
    """Fill the optional fields of a site dict.

    Parameters
    ----------
    site : dict

    Returns
    -------
    tuple
        (site, status)
    """
    status = 'ok'
    for k in REQ_FIELDS:
        if k not in site:
            status = 'invalid'
            break
    if 'base_url' not in site:
        if isinstance(site, str): print(site)
        if site.get('is_alive', True) is False:
            site['is_alive'] = False
            site['base_url'] = 'http://' + site['domain'] + '/'
            status = 'inactive'
        else:
            base_url = infer_base_url(site['domain'])
            if base_url is None:
                logger.warning('Domain %s is inactive!', site['domain'])
                site['base_url'] = 'http://' + site['domain'] + '/'
                site['is_alive'] = False
                status = 'inactive'
            else:
                if owns_url(site['domain'], base_url):
                    site['base_url'] = base_url
                    site['is_alive'] = True
                else:
                    status = 'redirected'
                    site['base_url'] = base_url
                    site['is_alive'] = True
    fill_rules(site)
    return (site, status)


def build_order_by_expr(ob_cls, ob_kw):
    """Internal method to build SQLAlchemy order-by expression from dict."""
    if ob_kw is None or len(ob_kw) == 0:
        return None
    elif len(ob_kw) == 1:
        for k, v in ob_kw.items():
            if v.lower() == 'asc':
                return getattr(ob_cls, k).asc()
            elif v.lower() == 'desc':
                return getattr(ob_cls, k).desc()
            else:
                print(('Invalidate order keyword: %s', ob_kw))
                sys.exit(2)
    else:
        print(('Support one column order by only: %s', ob_kw))
        sys.exit(2)


class SiteCmd(HoaxyCommand):
    """
Usage:
  hoaxy site --load-domains --site-type=<t>
             [(--ignore-inactive | --force-inactive)]
             [ --ignore-redirected] <file>
  hoaxy site --load-sites
             [(--ignore-inactive | --force-inactive)]
             [--ignore-redirected] <file>
  hoaxy site --add [(--ignore-inactive | --force-inactive) --ignore-redirected]
             [(--tag-source=<ts> --site-tag=<t2>...)]
             [--alternate-domain=<d2>...]
             [--name=<n>] --domain=<d> --site-type=<t1>
  hoaxy site --add-site-tags
             (--name=<n> | --domain=<d>) --tag-source=<ts> --site-tag=<t2>...
  hoaxy site --add-alternate-domains
             (--name=<n> | --domain=<d>) --alternate-domain=<d2>...
  hoaxy site --replace-site-tags
             (--name=<n> | --domain=<d>) --tag-source=<ts> --site-tag=<t2>...
  hoaxy site --replace-alternate-domains
             (--name=<n> | --domain=<d>) --alternate-domain=<d2>
  hoaxy site --disable (--name=<n> | --domain=<d>)
  hoaxy site --enable (--name=<n> | --domain=<d>)
  hoaxy site --bulk-enable [--exclusive] (--names=<n>... | --domains=<d>...)
  hoaxy site --bulk-disable  (--names=<n>... | --domains=<d>...)
  hoaxy site --status [--include-disabled]
  hoaxy site --dump <file>

This command provides site management functions. Roughly three categories:
add sites, alter sites, and check and dump sites.

(1) Add sites. You can add sites either by loading data from files or
input site attributes as command arguments:
--load-domains      Add or update sites by loading data from domains file.
--load-sites        Add or update sites by loading data from sites file.
--add-site          Simple way to add one site entry.

Note: when loading data from domains file, the existed site will not be
overwritten, while loading data from sites, the existed site will be
overwritten.

Loading options for the above three commands are:
--ignore-inactive   If base URL is not provided, we infer it by its domain.
                      When this option is set, ignore inactive domain.
--force-inactive    When this option is set and base_url of site cannot
                      determine, force to add this site and set base_url as
                      'http://' + domain + '/'
--ignore-redirected
                    When this option is set, ignore site whose domain is
                        redirected

If you use --load-domains, you also need to specify the site type by:
--site-type=<t1>    The type of the adding site

If you use --add-site command, you can specify optional attributes
of a site by:
--tag-source=<ts>   From which source you mark the site with these tags
                      If you tagged the site your self, you can name your own
                      source name
--site-tag=<t2>...  The tag of the adding site, you can add more tags by
                    repeating this option
--alternate-domain=<d2>...
                    Alternate domain or secondary domain of a site

(2) Alter sites: alter attributes of a site
--add-site-tag      Add new tags to an existed site
--replace-site-tag  Replace tags of an existed site with new tags
--add-secondary-domain
                    Add new secondary domain to an existed site
--replace-secondary-domain
                    Replace secondary domains of a site with new one
--disable-site      Set this site as disabled, meaning crawling (cron jobs)
                    and tracking (real time, e.g., twitter streaming)
                    processes will ignore this site. Restart of tracking
                    process is required
--enable-site       Set this site as enabled, meaning crawling and tracking
                    will be aware of this site. Restart of tracking process
                    is required

These commands need you provide info to locate a site by:
--name=<n>          Name of the site
--domain=<d>        Primary domain of a site

(3) Check status and dump
--status            Send HTTP requests to the sites to check the status of
                      sites (being alive, inactive or redirected)
--dump              Dump sites into a YAML file


The --status command can work with option:
--include-disabled  Whether include sites that are marked as disabled

Other options are:
-h --help               Show help.

Examples (`||` represents the continue of commands, you can ignore when using):
    1. At the first stage, you would like to add some claim sites to track.
       If sites own only one domain or you care only one the primary domain.
       (if exclusive is specified, all the existing sites
        will be disabled for the same site-type you specified)
       You can provide a list of domains:
        hoaxy site --load-domain [--exclusive] --site-type=claim [YOUR_DOMAINS_FILE]

       Or if you have edit your sites.yaml file, load it:
        hoaxy site --load-site [YOUR_SITES_FILE]

    2. Sometime you would like to add one specified site:
        hoaxy site --add --domain=abcd.com --site-type=claim
            || --tag-source=mytags --site-tag=fake

    3. Occasionally you would like to alter attribute of a site:
       Or add new tags to one site:
        hoaxy site --add-site-tag --name=abcd.com
            || --tag-source=opensource --site-tag=satire

       Or replace new tags to one site:
        hoaxy site --replace-site-tag --name=abcd.com
            || --tag-source=opensource --site-tag=bias

       Or add alternate domains to one site:
        hoaxy --add-alternate-domain --name=a1.com --alternate-domain=a2.com

       Or stop track of one site
        hoaxy site --disable --name=abcd.com
        (Restart of tracking processes)

       Or re-enable one site:
        hoaxy site --enable --name=abcd.com
        (Restart of tracking processes)

       Or enable list of sites:
        hoaxy site --bulk-enable --names=abcd.com, cnn.com
        hoaxy site --bulk-enable [--exclusive] --names=abcd.com, cnn.com (if exclusive is specified, all the existing sites
        will be disabled. Only enables listed sites)
        (Restart of tracking processes)

       Or disable list of sites:
        hoaxy site --bulk-disable --names=abcd.com, cnn.com
        (Restart of tracking processes)

    4. Dump sites into a YAML file
        hoaxy site --dump sites.dump.yaml
    """
    name = 'site'
    short_description = 'News site management: add, alter and dump'

    @classmethod
    def load_domains(cls,
                     session,
                     fn,
                     site_type,
                     ignore_inactive=False,
                     force_inactive=False,
                     ignore_redirected=False,
                     exclusive=False):
        if exclusive:
            # disable existing domains of the same site type
            sites = get_site_tuples(session)
            for site in sites:
                if site.site_type is site_type:
                    cls.disable_site(session, site)
        logger.info('Sending HTTP requests to infer base URLs ...')
        with open(fn, 'r') as f:
            site_tuples = [(n + 1, line) + parse_domain(line, site_type)
                           for n, line in enumerate(f)
                           if not is_comment_line(line)]
        invalid_flag = False
        inactive_flag = False
        redirected_flag = False
        for n, line, site, status in site_tuples:
            line = line.strip('\n')
            if status == 'invalid':
                invalid_flag = True
                logger.error('line %i %r, invalid domain', n, line)
            elif status == 'inactive':
                inactive_flag = True
                logger.warning('line %i %r, domain inactive!', n, line)
            elif status == 'redirected':
                redirected_flag = True
                logger.warning('line %i %r, domain redirected to %s!', n, line,
                               site['base_url'])
        if invalid_flag is True or \
                (inactive_flag is True and (ignore_inactive is False and
                                            force_inactive is False)) or \
                (redirected_flag is True and ignore_redirected is False):
            logger.error("""Please fix the warnings or errors above! \
Edit domains, or use --ignore-redirected to handle redirected domains', \
or Use --ignore-inactive or --force-inactive  to handle inactive domains""")
            raise SystemExit(2)
        for n, line, site, status in site_tuples:
            if status == 'inactive' and ignore_inactive is True:
                continue
            elif status == 'redirected' and ignore_redirected is True:
                continue
            else:
                get_or_create_m(
                    session, Site, site, fb_uk='domain', onduplicate='ignore')
                logger.debug('Insert or update site %s', site['domain'])

    @classmethod
    def load_sites(cls, session, fn, force_inactive, ignore_inactive,
                   ignore_redirected):
        """Load site.yaml into database."""
        with open(fn, 'r') as f:
            sites = yaml.load(f, Loader=yaml.FullLoader)['sites']
        invalid_flag = False
        inactive_flag = False
        redirected_flag = False
        site_tuples = [parse_site(site) for site in sites]
        for site, status in site_tuples:
            if status == 'invalid':
                invalid_flag = True
                logger.error('Site %s absence of required fields (%s)!', site,
                             REQ_FIELDS)
            elif status == 'inactive':
                inactive_flag = True
                logger.warning('Site %s is inactive now!', site['name'])
            elif status == 'redirected':
                redirected_flag = True
                logger.warning('Site %s redirects to %s!', site['name'],
                               site['base_url'])
        if invalid_flag is True or \
                (inactive_flag is True and (ignore_inactive is False and
                                            force_inactive is False)) or \
                (redirected_flag is True and ignore_redirected is False):
            logger.error("""Please fix the warnings or errors above! \
Edit domains, or use --ignore-redirected to handle redirected domains', \
or Use --ignore-inactive or --force-inactive  to handle inactive domains""")
            raise SystemExit(2)
        for site, status in site_tuples:
            if status == 'inactive' and ignore_inactive is True:
                continue
            elif status == 'redirected' and ignore_redirected is True:
                continue
            else:
                alternate_domains = site.pop('alternate_domains', [])
                site_tags = site.pop('site_tags', [])
                get_or_create_msite(
                    session,
                    site,
                    alternate_domains=alternate_domains,
                    site_tags=site_tags,
                    onduplicate='update')
                logger.debug('Insert or update site %s', site['domain'])

    @classmethod
    def add_site(cls,
                 session,
                 domain,
                 site_type,
                 name=None,
                 site_tags=[],
                 tag_source=None,
                 alternate_domains=[],
                 ignore_inactive=False,
                 force_inactive=False,
                 ignore_redirected=False):
        """Add one site."""
        site, status = parse_domain(domain, site_type)
        if name is not None:
            site['name'] = name
        error_flag = False
        if status == 'invalid':
            logger.error('Not a valid domain %s!', domain)
            error_flag = True
        elif status == 'inactive':
            if ignore_inactive is True:
                logger.warning('Domain %s is inactive now, ignored', domain)
            else:
                logger.error(
                    """Domain %r is inactive now, cannot determine \
the base URL. Try `--force-inactive` to force insert.""", domain)
                error_flag = True
        elif status == 'redirected':
            if 'ignore_redirected' is True:
                logger.warning('Domain %r is redirected to %s, ignored',
                               domain, site['base_url'])
            else:
                error_flag = True
                logger.error(
                    """Domain %r is redirected to %r \
Use the redirected domain as primary domain and add %r as secondary domain""",
                    domain, site['base_url'], domain)
        else:
            site_tags = [dict(source=tag_source, name=t) for t in site_tags]
            get_or_create_msite(
                session,
                site,
                site_tags=site_tags,
                alternate_domains=alternate_domains,
                onduplicate='ignore')
        if error_flag:
            raise SystemExit(1)
        else:
            logger.info('Done!')

    @classmethod
    def add_site_tags(cls, session, msite, source, tags):
        """Add site_tags for a site."""
        owned_tags = [(mt.name, mt.source) for mt in msite.site_tags]
        fb_uk = ['name', 'source']
        for tag in tags:
            tag_data = dict(name=tag, source=source)
            if (tag, source) in owned_tags:
                logger.warning('Site %r already contains tag %r!', msite.name,
                               tag_data)
            else:
                mtag = get_or_create_m(session, SiteTag, tag_data, fb_uk=fb_uk)
                msite.site_tags.append(mtag)
                logger.info('Added tag %r to Site %r', tag_data, msite.name)
                session.commit()

    @classmethod
    def replace_site_tags(cls, session, msite, source, tags):
        """Replace old site_tags with new ones."""
        adding_tags = [(t, source) for t in tags]
        owned_tags = [(mt.name, mt.source) for mt in msite.site_tags]
        for t in tags:
            if (t, source) not in owned_tags:
                tag_data = dict(name=t, source=source)
                fb_uk = ['name', 'source']
                mtag = get_or_create_m(session, SiteTag, tag_data, fb_uk=fb_uk)
                msite.site_tags.append(mtag)
        for mt in msite.site_tags:
            if (mt.name, mt.source) not in adding_tags:
                session.delete(mt)
        session.commit()
        logger.info('Replace site tags for site %r from %r to %r', msite.name,
                    owned_tags, adding_tags)

    @classmethod
    def add_alternate_domains(cls, session, msite, domains):
        """Add Alternate domains for a site."""
        owned_domains = [md.name for md in msite.alternate_domains]
        for d in domains:
            md = session.query(AlternateDomain).filter_by(name=d).one_or_none()
            if md is not None:
                logger.error('AlternateDomain %r is already owned by site %r',
                             d, md.site.name)
                return
            elif d in owned_domains:
                logger.warning('Site %r already contains the domain %r!',
                               msite.name, d)
            else:
                msite.alternate_domains.append(AlternateDomain(name=d))
                logger.info('Added %r to site %r as alternate domain', d,
                            msite.name)
                session.commit()

    @classmethod
    def replace_alternate_domains(cls, session, msite, domains):
        """Replace alternate domains with new ones."""
        owned_domains = [md.name for md in msite.alternate_domains]
        for d in domains:
            if d not in owned_domains:
                md = session.query(AlternateDomain).filter_by(
                    name=d).one_or_none()
                if md is not None:
                    logger.error(
                        'AlternateDomain %r is already owned by site %r', d,
                        md.site.name)
                    return
                else:
                    msite.alternate_domains.append(AlternateDomain(name=d))
                    session.commit()
        for md in msite.alternate_domains:
            if md.name not in domains:
                session.delete(md)
                session.commit()
        logger.info('Replaced alternate domains of Site %r from %r to %r',
                    msite.name, owned_domains, domains)

    @classmethod
    def enable_site(cls, session, msite):
        """Set site status as enabled."""
        if msite.is_enabled is True:
            logger.warning('Site name=%s, domain=%s is already enabled!',
                           msite.name, msite.domain)
        else:
            msite.is_enabled = True
            session.commit()
            logger.warning(
                """Site %r is enabled. \
You need to restart your tracking process!""", msite.name)

    @classmethod
    def disable_site(cls, session, msite):
        """Set the status of a site as disabled."""
        if msite.is_enabled is False:
            logger.warning('Site name=%s, domain=%s is already disabled!',
                           msite.name, msite.domain)
        else:
            msite.is_enabled = False
            session.commit()
            logger.warning(
                """Site %r is disabled. \
You need to restart your tracking process!""", msite.name)

    @classmethod
    def site_status(cls, session, include_disabled=False):
        """Check the status of all sites in the database."""
        inactive = []
        redirected = []
        stable = []
        if include_disabled is True:
            q = 'SELECT id, domain FROM site ORDER BY domain'
        else:
            q = 'SELECT id, domain FROM site WHERE is_enabled IS TRUE'\
                + ' ORDER BY domain'
        for sid, domain in session.execute(q).fetchall():
            base_url = infer_base_url(domain)
            if base_url is None:
                inactive.append(dict(id=sid, domain=domain))
            else:
                if owns_url(domain, base_url) is True:
                    stable.append(
                        dict(id=sid, domain=domain, base_url=base_url))
                else:
                    redirected.append(
                        dict(id=sid, domain=domain, base_url=base_url))
        logger.info('Stable sites: %s', pprint.pformat(stable))
        for o in stable:
            session.query(Site).filter_by(id=o['id'])\
                .update(dict(base_url=o['base_url']),
                        synchronize_session=False)
        session.commit()
        logger.info('Inactive sites: %s', pprint.pformat(inactive))
        logger.info('Redirected sites: %s', pprint.pformat(redirected))

    @classmethod
    def dump(cls, session, yaml_fn):
        """Dump all sites in the database into a yaml file."""
        ob_expr = Site.id.asc()
        msites = get_msites(session, fb_kw=None, ob_expr=ob_expr)
        r = []
        for ms in msites:
            site = CommentedMap()
            site['name'] = ms.name
            site['domain'] = ms.domain
            site['site_type'] = ms.site_type
            site['base_url'] = ms.base_url
            site['site_tags'] = [
                dict(name=t.name, source=t.source) for t in ms.site_tags
            ]
            site['alternate_domains'] = [
                dict(name=ad.name, is_alive=ad.is_alive)
                for ad in ms.alternate_domains
            ]
            site['is_alive'] = ms.is_alive
            site['is_enabled'] = ms.is_enabled
            article_rules = CommentedMap()
            site['article_rules'] = article_rules
            article_rules['url_regex'] = ms.article_rules['url_regex']
            article_rules['update'] = []
            article_rules['archive'] = []
            for rule in ms.article_rules['update']:
                u = CommentedMap()
                u['spider_name'] = rule['spider_name']
                u['spider_kwargs'] = rule['spider_kwargs']
                article_rules['update'].append(u)
            for rule in ms.article_rules['archive']:
                a = CommentedMap()
                a['spider_name'] = rule['spider_name']
                a['spider_kwargs'] = rule['spider_kwargs']
                article_rules['archive'].append(a)
            r.append(site)

        ys = ruamel.yaml.round_trip_dump(r)
        head_comments = """\
# This file is generate by hoaxy site --dump command.
# To understand the sites data structure, please read sites.readme.md, which
# should locate under hoaxy/data/manuals/.

"""
        # out_put
        with open(yaml_fn, 'w') as f:
            f.write(head_comments + ys)
        logger.info('Sites dumped into YAML file %s', yaml_fn)

    @classmethod
    def run(cls, args):
        """Overriding method as the entry point of this command."""
        session = Session(expire_on_commit=False)
        # session = Session()
        # expand user home for the file
        if args['<file>'] is not None:
            args['<file>'] = os.path.expanduser(args['<file>'])
        # --load-domains commands
        if args['--load-domains'] is True:
            configure_logging(
                'site.load-domains',
                console_level=args['--console-log-level'],
                file_level='WARNING')
            fn = args.get('<file>', join(HOAXY_HOME, 'domains.txt'))
            logger.info('Loading data from file %r', fn)
            cls.load_domains(
                session,
                fn,
                site_type=args['--site-type'],
                ignore_inactive=args['--ignore-inactive'],
                force_inactive=args['--force-inactive'],
                ignore_redirected=args['--ignore-redirected'],
                exclusive=args['--exclusive'])
        # --load-sites commands
        elif args['--load-sites'] is True:
            configure_logging(
                'site.load-sites',
                console_level=args['--console-log-level'],
                file_level='WARNING')
            fn = args.get('<file>', join(HOAXY_HOME, 'sites.yaml'))
            logger.info('Loading data from file %r', fn)
            cls.load_sites(
                session,
                fn,
                ignore_inactive=args['--ignore-inactive'],
                force_inactive=args['--force-inactive'],
                ignore_redirected=args['--ignore-redirected'])
        # --add commands
        elif args['--add'] is True:
            configure_logging(
                'site.add',
                console_level=args['--console-log-level'],
                file_level='WARNING')
            msite = qquery_msite(session, domain=args['--domain'])
            if msite is not None:
                logger.warning('Site %s already exists!', args['--domain'])
            else:
                cls.add_site(
                    session,
                    domain=args['--domain'],
                    site_type=args['--site-type'],
                    name=args['--name'],
                    tag_source=args['--tag-source'],
                    site_tags=args['--site-tag'],
                    alternate_domains=args['--alternate-domain'],
                    ignore_inactive=args['--ignore-inactive'],
                    force_inactive=args['--force-inactive'],
                    ignore_redirected=args['--ignore-redirected'])
        # --add-site-tags
        elif args['--add-site-tags'] is True:
            configure_logging(
                'site.add-site-tags',
                console_level=args['--console-log-level'],
                file_level='WARNING')
            if args['--name'] is not None:
                site_identity = args['--name']
            else:
                site_identity = args['--domain']
            msite = qquery_msite(
                session, name=args['--name'], domain=args['--domain'])
            if msite is None:
                logger.warning('Site %s does not exist!', site_identity)
            else:
                cls.add_site_tags(session, msite, args['--tag-source'],
                                  args['--site-tag'])
        # --replace-site-tags
        elif args['--replace-site-tags'] is True:
            configure_logging(
                'site.repalce-site-tags',
                console_level=args['--console-log-level'],
                file_level='WARNING')
            if args['--name'] is not None:
                site_identity = args['--name']
            else:
                site_identity = args['--domain']
            msite = qquery_msite(
                session, name=args['--name'], domain=args['--domain'])
            if msite is None:
                logger.warning('Site %s does not exist!', site_identity)
            else:
                cls.replace_site_tags(session, msite, args['--tag-source'],
                                      args['--site-tag'])
        # --add-alternate-domains
        elif args['--add-alternate-domains'] is True:
            configure_logging(
                'site.add-alternate-domains',
                console_level=args['--console-log-level'],
                file_level='WARNING')
            if args['--name'] is not None:
                site_identity = args['--name']
            else:
                site_identity = args['--domain']
            msite = qquery_msite(
                session, name=args['--name'], domain=args['--domain'])
            if msite is None:
                logger.warning('Site %s does not exist!', site_identity)
            else:
                cls.add_alternate_domains(session, msite,
                                          args['--alternate-domain'])
        # --replace-alternate-domains
        elif args['--replace-alternate-domains'] is True:
            configure_logging(
                'site.replace-alternate-domains',
                console_level=args['--console-log-level'],
                file_level='WARNING')
            if args['--name'] is not None:
                site_identity = args['--name']
            else:
                site_identity = args['--domain']
            msite = qquery_msite(
                session, name=args['--name'], domain=args['--domain'])
            if msite is None:
                logger.warning('Site %s does not exist!', site_identity)
            else:
                cls.replace_alternate_domains(session, msite,
                                              args['--alternate-domain'])
        elif args['--disable'] is True:
            configure_logging(
                'site.disable',
                console_level=args['--console-log-level'],
                file_level='WARNING')
            if args['--name'] is not None:
                site_identity = args['--name']
            else:
                site_identity = args['--domain']
            msite = qquery_msite(
                session, name=args['--name'], domain=args['--domain'])
            if msite is None:
                logger.warning('Site %s does not exist!', site_identity)
            else:
                cls.disable_site(session, msite)
        elif args['--enable'] is True:
            configure_logging(
                'site.enable',
                console_level=args['--console-log-level'],
                file_level='WARNING')
            if args['--name'] is not None:
                site_identity = args['--name']
            else:
                site_identity = args['--domain']
            msite = qquery_msite(
                session, name=args['--name'], domain=args['--domain'])
            if msite is None:
                logger.warning('Site %s does not exist!', site_identity)
            else:
                cls.enable_site(session, msite)
        # bulk enable sites and domains
        elif args['--bulk-enable'] is True:
            configure_logging(
                'site.bulk-enable',
                console_level=args['--console-log-level'],
                file_level='WARNING')
            if args['--exclusive'] is True:
                ob_expr = Site.id.asc()
                msites = get_msites(session, fb_kw=None, ob_expr=ob_expr)
                # disable existing sites
                for existing_site in msites:
                    cls.disable_site(session, existing_site)
            if args['--names'] is not None:
                site_list = args['--names']
                for site in site_list:
                    msite = qquery_msite(session, name=site, domain=None)
                    if msite is None:
                        logger.warning('Site %s does not exist!', site)
                    else:
                        cls.enable_site(session, msite)
            else:
                domain_list = args['--domains']
                for domain in domain_list:
                    msite = qquery_msite(session, name=None, domain=domain)
                    if msite is None:
                        logger.warning('Site %s does not exist!', domain)
                    else:
                        cls.enable_site(session, msite)

        # bulk disable sites and domains
        elif args['--bulk-disable'] is True:
            configure_logging(
                'site.bulk-disable',
                console_level=args['--console-log-level'],
                file_level='WARNING')
            if args['--names'] is not None:
                site_list = args['--names']
                for site in site_list:
                    msite = qquery_msite(session, name=site, domain=None)
                    if msite is None:
                        logger.warning('Site %s does not exist!', site)
                    else:
                        cls.disable_site(session, msite)
            else:
                domain_list = args['--domains']
                for domain in domain_list:
                    msite = qquery_msite(session, name=None, domain=domain)
                    if msite is None:
                        logger.warning('Site %s does not exist!', domain)
                    else:
                        cls.disable_site(session, msite)
        # --status
        elif args['--status'] is True:
            configure_logging(
                'site.status',
                console_level=args['--console-log-level'],
                file_level='WARNING')
            if args['--include-disabled'] is True:
                cls.site_status(session, True)
            else:
                cls.site_status(session, False)
        # --dump
        elif args['--dump'] is True:
            configure_logging(
                'site.status',
                console_level=args['--console-log-level'],
                file_level='INFO')
            cls.dump(session, args['<file>'])

        session.close()
