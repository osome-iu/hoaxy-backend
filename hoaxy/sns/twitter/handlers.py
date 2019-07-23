# -*- coding: utf-8 -*-
"""Handlers that specify how to store tweet.
"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

import gzip
import logging
import sys
try:
    import simplejson as json
except ImportError:
    import json

logger = logging.getLogger(__name__)


class BaseHandler(object):
    """The abstract class of the handler."""

    def process_one(self, jd):
        """The main function of the handler.

        Specify how to process one tweet.

        Parameters
        ----------
        jd : json
            The tweet json object.
        """
        raise NotImplementedError

    def close(self):
        """How to close the handler."""
        raise NotImplementedError

    def __str__(self):
        """How to string format the handler."""
        raise NotImplementedError


class FileHandler(BaseHandler):
    """A file implementation to handle tweet."""

    def __init__(self, filepath):
        """Constructor.

        Parameters
        ----------
        filepath : string
            The filename to save into.
        """
        self.filepath = filepath
        self.fp = None
        self._openfile()

    def _openfile(self):
        """Internal function to open file. Mode should be append and text"""
        if self.filepath == '-':
            self.fp = sys.stdout
        elif self.filepath.endswith(".gz"):
            self.fp = gzip.open(filename=self.filepath, 'at')
        else:
            self.fp = open(self.filepath, 'at')

    def process_one(self, jd):
        """Dump json as string, each object per line."""
        try:
            line = json.dumps(jd)
        except Exception as e:
            logger.error('JSON dumps: %s', e)
            return
        try:
            self.fp.write(line)
            self.fp.write('\n')
            self.fp.flush()
        except Exception as e:
            logger.error(e)
            self.fp.close()
            raise

    def close(self):
        """Close the file."""
        self.fp.close()

    def __str__(self):
        """Give core information about this handler."""
        return 'FileHandler: path={}'.format(self.filepath)


class QueueHandler(BaseHandler):
    """A Queue implementation to handle tweet."""

    def __init__(self, queue):
        """Constructor of QueueHandler.

        Parameters
        ----------
        queue : queue object.
        """
        self.queue = queue

    def process_one(self, jd):
        """The implementation of queuehandler is to put tweet into queue."""
        self.queue.put(jd)

    def close(self):
        """Not implement."""
        pass

    def __str__(self):
        return 'QueueHandler'
