# -*- coding: utf-8 -*-
"""Hoayx command config implementation."""

#
# written by Chengcheng Shao <sccotte@gmail.com>

from hoaxy import HOAXY_HOME
from hoaxy.commands import HoaxyCommand
from pkg_resources import resource_listdir, resource_filename
import logging
import os
import os.path
import shutil

logger = logging.getLogger(__name__)


class Config(HoaxyCommand):
    """
usage:
  hoaxy config [--home=<h>]

Generate sample files and put them into hoaxy home folder. By default,
HOAXY_HOME is '~/.hoaxy/', unless you set the environment HOAXY_HOME.
If --home=<h> option is set, files will be put into <h>. If <h> does not exist,
try to create it. Remeber, you must set environment HOAXY_HOME=<h>, if you
would like to make <h> as your hoaxy home folder.

--home=<h>      The location where samples will be put into.
-h --help       Show help.

Purpose of sample files:
  conf.sample.yaml                  Hoaxy main configuration
  sites.sample.yaml                 Define which sites to track
  domains_claim.sample.txt          Claim domains to track
  domains_factchecking.sample.txt   Fact checking domains to track
  crontab.sample.txt                Set up cron jobs.

Examples:

    1. Generate samples to default hoaxy home foler(~/.hoaxy/)
        hoaxy config

    2. Genetate samples to location '/home/apps/hoaxy/'
        hoaxy config --home=/home/app/hoaxy/

    Note: to make use of these samples, you need to edit, rename, and
    of course, set environment HOAXY_HOME=/home/apps/hoaxy/
    """
    name = 'config'
    short_description = 'Generate sample of configuration files'

    @classmethod
    def run(cls, args):
        samples = """
conf.sample.yaml                    Hoaxy configuration file
domains_claim.sample.txt            Claim domains
domains_factchecking.sample.txt     Factchecking domains
site.sample.yaml                    Claim and/or factchecking sites
crontab.sample.txt                  Crontab sample
"""
        if args['--home'] is None:
            hoaxy_home = HOAXY_HOME
            msg = """
Sample files are put into the default location:
  '{}'.
Please edit and rename sample files to make Hoaxy work with them.
{}"""
            msg = msg.format(hoaxy_home, samples)
        else:
            hoaxy_home = os.path.expanduser(args['--home'])
            if not hoaxy_home.endswith('/'):
                hoaxy_home += '/'
            msg = """
Sample files are put into folder
'{}'.
You need to set environment
HOAXY_HOME={}
to activate this path.

Also please edit and rename samples to make Hoaxy work with them.
{}"""
            msg = msg.format(hoaxy_home, hoaxy_home, samples)
        if not os.path.exists(hoaxy_home):
            try:
                org_umask = os.umask(0)
                os.makedirs(hoaxy_home, 0o755)
            finally:
                os.umask(org_umask)
        samples = resource_listdir('hoaxy.data', 'samples')
        for sample in samples:
            if not sample.startswith('__init__.') or not sample.startswith('__pycache__'):
                sample = resource_filename('hoaxy.data.samples', sample)
                shutil.copy(sample, hoaxy_home)
                os.chmod(
                    os.path.join(hoaxy_home, os.path.basename(sample)), 0o644)
        print(msg)
