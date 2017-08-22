# -*- coding: utf-8 -*-
"""Hoaxy subcommand Init implementation.

Initialize the tables of database.
"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

from datetime import datetime
from hoaxy import HOAXY_HOME
from hoaxy.commands import HoaxyCommand
from hoaxy.commands.site import SiteCmd
from hoaxy.database import Session, ENGINE
from hoaxy.database.functions import get_or_create_m
from hoaxy.database.models import Base, Site
from hoaxy.database.models import Platform, TWITTER_PLATFORM_DICT
from hoaxy.database.models import WEB_PLATFORM_DICT
from hoaxy.utils.log import configure_logging
from os.path import isfile
from os.path import join
from sqlalchemy import or_
import logging
import pprint

logger = logging.getLogger(__name__)


class Init(HoaxyCommand):
    """
usage:
  hoaxy init [--force-drop]
             [(--ignore-inactive | --force-inactive)]
             [--ignore-redirected]

Initialize database and load site data from hoaxy home if exists. The site
data files are located at hoaxy home. They are: sites.yaml, domains_claim.txt,
domains_factchecking.txt.

There is one option related to table drop:
--force-drop        If set, drop existed tables and recreate them. This
                      option may not work if there are cascading problem.

Other options are used to guide the loading of site data, you can also see
`hoaxy site -h` command for more information about site data:
--ignore-inactive   If base URL is not provided, we infer it by its domain
                      When this option is set, ignore inactive domain.
--force-inactive    When this option is set and base_url of site cannot
                      determine, force to add this site and set base_url as
                      'http://' + domain + '/'
--ignore-redirected
                    When this option is set, ignore site whose domain is
                        redirected


Examples:

  1. Initialize database and load all possible site data:
        hoaxy init --force-inactive --ignore-redirected
    """
    name = 'init'
    short_description = 'Initialize DB tables and load site data if exists'

    @classmethod
    def init(cls,
             session,
             force_drop,
             ignore_inactive=False,
             force_inactive=False,
             ignore_redirected=False):
        configure_logging('init', console_level='INFO', file_level='WARNING')
        dt_before = datetime.utcnow()
        logging.info('Creating database tables:')
        if force_drop is True:
            logging.warning('Existed tables would be dropped and recreated!')
            Base.metadata.drop_all(ENGINE)
        else:
            logging.warning('Ignore existed tables')
        Base.metadata.create_all(ENGINE)
        logging.info('Inserting platforms if not exist')
        get_or_create_m(session, Platform, TWITTER_PLATFORM_DICT, fb_uk='name')
        get_or_create_m(session, Platform, WEB_PLATFORM_DICT, fb_uk='name')
        logging.info('Trying to load site data:')
        dc_file = join(HOAXY_HOME, 'domains_claim.txt')
        df_file = join(HOAXY_HOME, 'domains_factchecking.txt')
        site_file = join(HOAXY_HOME, 'sites.yaml')
        if isfile(dc_file) is True:
            logging.info('Claim domains %s found', dc_file)
            SiteCmd.load_domains(
                session,
                dc_file,
                site_type='claim',
                ignore_inactive=ignore_inactive,
                force_inactive=force_inactive,
                ignore_redirected=ignore_redirected)
        else:
            logging.info('Claim domains %s not found', dc_file)
        if isfile(df_file) is True:
            logging.info('Fact checking domains %s found', df_file)
            SiteCmd.load_domains(
                session,
                df_file,
                site_type='fact_checking',
                ignore_inactive=ignore_inactive,
                force_inactive=force_inactive,
                ignore_redirected=ignore_redirected)
        else:
            logging.info('Fact checking domains %s not found', df_file)

        if isfile(site_file) is True:
            logging.info('Site file %s found', site_file)
            SiteCmd.load_sites(
                session,
                site_file,
                ignore_inactive=ignore_inactive,
                force_inactive=force_inactive,
                ignore_redirected=ignore_redirected)
        else:
            logging.info('Site file %s not found', site_file)
        sites = session.query(
            Site.domain, Site.site_type, Site.base_url).filter(
                or_(Site.created_at > dt_before, Site.updated_at >
                    dt_before)).order_by(Site.id).all()
        logger.info("Added or updated sites are:\n %s", pprint.pformat(sites))
        logger.info("Done.")

    @classmethod
    def run(cls, args):
        """Overriding method as the entry point of this command."""
        session = Session()
        cls.init(
            session,
            args['--force-drop'],
            ignore_inactive=args['--ignore-inactive'],
            force_inactive=args['--force-inactive'],
            ignore_redirected=args['--ignore-redirected'])
