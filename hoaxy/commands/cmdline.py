# -*- coding: utf-8 -*-
"""Hoaxy commandline entry point.
(1) This is the main entry point for the whole commandline interface
(2) The main function would iterate the whole package of commands to find
    all available comands class.

"""

#
# written by Chengcheng Shao <sccotte@gmail.com>

from docopt import docopt
from hoaxy import DEFAULT_HOAXY_HOME
from hoaxy import HOAXY_CONF
from hoaxy import HOAXY_HOME
from hoaxy import VERSION
from hoaxy import commands
from hoaxy.commands import HoaxyCommand
from hoaxy.utils import list_cls_under_mod
import os.path
import sys

# Usage message of hoaxy
HOAXY_USAGE = """\
Usage:
  hoaxy <command> [<args>...] [--help]
  hoaxy -h | --help
  hoaxy -v | --version

Subcommands are:
{cmds_short_description}

For subcommands, use `hoaxy <command> --help` to get the usage of the
subcommand <command>!
"""


def format_cmds_descriptions(cmds_cls):
    """Format short descriptions of all subcommands.

    Parameters
    ----------
        cmds_cls : class

    Returns
    -------
        formatted descriptions of subcommands.

    """
    cmds_name_length = [len(k) for k, c in cmds_cls.iteritems()]
    max_name_length = max(cmds_name_length)
    formatter = '{0:%s}{1}\n' % (max_name_length + 4)
    cmds_short_description = ''
    for k, c in cmds_cls.iteritems():
        cmds_short_description += formatter.format(k, c.short_description)
    return cmds_short_description


def main(argv=None):
    """The main funtion (entry point).

    Parameters
    ----------
        argv : list
            The argument list. By default, it should be None and sys.argv is
            used.

    Notes
    -----
    The logical of this functions is:
        (1) Check the existence of configuration file.
        (2) Check and execute subcommand.
    """
    cmds_cls = list_cls_under_mod(commands, HoaxyCommand, 'name')
    cmds_short_description = format_cmds_descriptions(cmds_cls)
    error_msg = """
****************************  ERROR  ******************************************
NO HOAXY CONFIGURATION FILE FOUND: (DEFAULT '{}')!

(1) If you have put the conf.yaml to somewhere other than default location
    '{}',
    please set your enviroment variable HOAXY_HOME properly.
(2) Use hoaxy config [--home=<h>] to get a sample of configuration file
    named conf.sample.yaml
(3) Edit and rename conf.sample.yaml to conf.yaml. And you should edit and
    rename other sample files.

Currently, hoaxy uses default settings and may not work properly.
*******************************************************************************
""".format(os.path.join(HOAXY_HOME, 'conf.yaml'), DEFAULT_HOAXY_HOME)
    # no conf.yaml found, we need to prompt error message
    if (len(sys.argv) == 1 or sys.argv[1] != 'config') and \
            (not HOAXY_CONF.endswith('conf.yaml')):
        print(error_msg)
    args = docopt(
        HOAXY_USAGE.format(cmds_short_description=cmds_short_description),
        version=VERSION,
        options_first=True,
        argv=argv or sys.argv[1:])
    argv = [args['<command>']] + args['<args>']
    if args['<command>'] in cmds_cls:
        sub_args = docopt(cmds_cls[args['<command>']].__doc__, argv)
        cmds_cls[args['<command>']].run(sub_args)
    else:
        raise SystemExit("""Invalid subcommand! Try 'hoaxy -h' to list \
available subcommands""")


if __name__ == '__main__':
    main()
