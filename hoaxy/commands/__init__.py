# -*- coding: utf-8 -*-
"""Hoaxy commandline interface implementation.

(1) schema is used to validate the input of command
(2) docopt is used to parse args of command

"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

from hoaxy import CONF
from schema import Schema
import copy


class HoaxyCommand(object):
    """The base class for hoaxy commands.

    Notes
    -----
    `cls.name`, should be subcommand's name. For example, to run the command
        `hoaxy report [options]`, you must have a Hoaxycommand subclass that
        own class member name=report.

    `cls.conf`, configurations for the commands. Its default is a deepcopy from
        your configuration file.

    `cls.__doc__` is used by docopt to parse command arguments.

    `cls.run` is the entry point to call.
    """
    name = None
    short_description = 'This is the base hoaxy command'
    conf = copy.deepcopy(CONF)
    args_schema = Schema({
        object: object
    })

    @classmethod
    def run(cls, args):
        """Entry point for running commands.
        """
        raise NotImplementedError
