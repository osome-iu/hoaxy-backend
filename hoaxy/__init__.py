# Copyright 2016 by Chengcheng Shao.
# Email: shaoc@indiana.edu sccotte@gmail.com.
# All Rights Reserved.
#

from os.path import isfile
from os.path import join
from os.path import expanduser
from os.path import dirname
from os import getenv
from pkg_resources import resource_filename
import yaml

version_file = resource_filename('hoaxy', 'VERSION')

with open(version_file, 'r') as f:
    VERSION = f.read().decode('ascii').strip()
DEFAULT_HOAXY_HOME = join(expanduser('~'), '.hoaxy/')
DEFAULT_HOAXY_CONF = join(DEFAULT_HOAXY_HOME, 'conf.yaml')
HOAXY_HOME = getenv('HOAXY_HOME')
if HOAXY_HOME is None:
    HOAXY_HOME = DEFAULT_HOAXY_HOME
    HOAXY_CONF = DEFAULT_HOAXY_CONF
else:
    HOAXY_HOME = expanduser(HOAXY_HOME)
    HOAXY_CONF = join(HOAXY_HOME, 'conf.yaml')
if not isfile(HOAXY_CONF):
    HOAXY_CONF = resource_filename('hoaxy.data.samples', 'conf.sample.yaml')
    with open(HOAXY_CONF, 'r') as f:
        CONF = yaml.load(f)
else:
    with open(HOAXY_CONF, 'r') as f:
        CONF = yaml.load(f)
if not CONF['logging']['handlers']['file']['filename'].startswith('/'):
    CONF['logging']['handlers']['file']['filename'] = join(
        HOAXY_HOME, CONF['logging']['handlers']['file']['filename'])
if not CONF['lucene']['index_dir'].startswith('/'):
    CONF['lucene']['index_dir'] = join(HOAXY_HOME, CONF['lucene']['index_dir'])
# project root
BASE_DIR = dirname(dirname(__file__))
