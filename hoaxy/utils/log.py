# -*- coding: utf-8 -*-
"""Logging utilities."""
#
# written by Chengcheng Shao <sccotte@gmail.com>

import copy
import logging.config

from scrapy.logformatter import DROPPEDMSG, SCRAPEDMSG, LogFormatter
from twisted.python.failure import Failure

from hoaxy import CONF


class HoaxyFormatter(logging.Filter):
    """Re-structure logger name."""

    def __init__(self, pretty_name, override_inside_name_only=True):
        self.pretty_name = pretty_name
        self.override_inside_name_only = override_inside_name_only

    def filter(self, record):
        if self.override_inside_name_only is False:
            record.name = 'hoaxy({})'.format(self.pretty_name)
        else:
            if record.name.startswith('hoaxy.') or \
               record.name.startswith('root'):
                record.name = 'hoaxy({})'.format(self.pretty_name)
            elif not record.name.startswith('hoaxy('):
                outside_name = record.name
                record.name = 'hoaxy({})[{}]'.format(self.pretty_name,
                                                     outside_name)
        return True


class PrettyLogFormatter(LogFormatter):
    """Truncate message which are too long."""
    truncated_len = 100

    def scraped(self, item, response, spider):
        if isinstance(response, Failure):
            src = response.getErrorMessage()
        else:
            src = response
        truncated_item = dict()
        if item:
            for k in item.fields:
                if item.get(k) is not None:
                    if isinstance(item[k], (int, float, complex)):
                        truncated_item[k] = item[k]
                    elif isinstance(item[k], str):
                        truncated_item[k] = item[k][:self.truncated_len]
                        if len(item[k]) > self.truncated_len:
                            truncated_item[k] += ' ... (truncated)'
                    else:
                        truncated_item[k] = str(item[k])[:self.truncated_len]
                        if len(str(item[k])) > self.truncated_len:
                            truncated_item[k] += ' ... (truncated)'
                else:
                    truncated_item[k] = None
        return {
            'level': logging.DEBUG,
            'msg': SCRAPEDMSG,
            'args': {
                'src': src,
                'item': truncated_item,
            }
        }

    def dropped(self, item, exception, response, spider):
        truncated_item = dict()
        if item:
            for k in item.fields:
                if item.get(k) is not None:
                    if isinstance(item[k], (int, float, complex)):
                        truncated_item[k] = item[k]
                    elif isinstance(item[k], str):
                        truncated_item[k] = item[k][:self.truncated_len]
                        if len(item[k]) > self.truncated_len:
                            truncated_item[k] += ' ... (truncated)'
                    else:
                        truncated_item[k] = str(item[k])[:self.truncated_len]
                        if len(str(item[k])) > self.truncated_len:
                            truncated_item[k] += ' ... (truncated)'
                else:
                    truncated_item[k] = None
        return {
            'level': logging.DEBUG,
            'msg': DROPPEDMSG,
            'args': {
                'exception': exception,
                'item': truncated_item,
            }
        }


def configure_logging(cmd_name=None,
                      console_level='DEBUG',
                      file_level='INFO',
                      smtp_level='CRITICAL'):
    """Configure the logging.

    Parameters
    ----------
    cmd_name : string
        The name of command.
    console_level : string
        The logging level for console.
    file_level : string
        The logging level for file.
    smtp_level : string
        The logging level for smtp (email)
    """
    lconf = copy.deepcopy(CONF['logging'])
    if 'console' in lconf['handlers']:
        lconf['handlers']['console']['level'] = console_level
    if 'file' in lconf['handlers']:
        lconf['handlers']['file']['level'] = file_level
    if 'stmp' in lconf['handlers']:
        lconf['handlers']['stmp']['level'] = smtp_level
    logging.config.dictConfig(lconf)
    log_filter = HoaxyFormatter(cmd_name)
    for handler in logging.root.handlers:
        handler.addFilter(log_filter)
