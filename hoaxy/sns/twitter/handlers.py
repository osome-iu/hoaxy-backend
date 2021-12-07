# -*- coding: utf-8 -*-
"""Handlers that specify how to store tweet.
"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

import gzip
import logging
import queue
import sys
import threading
import time
from datetime import datetime
from os.path import join

from sqlalchemy.exc import OperationalError, SQLAlchemyError

from hoaxy.database import Session
from hoaxy.database.functions import get_platform_id
from hoaxy.database.models import N_PLATFORM_TWITTER
from hoaxy.sns.twitter.parsers import Parser

try:
    import simplejson as json
except ImportError:
    import json

logger = logging.getLogger(__name__)


class BaseHandler():
    """The abstract class of the handler. This abstract class provides the
    basic operation interfaces to hanlding the tweets.
    """

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
            self.fp = gzip.open(self.filepath, 'at')
        else:
            self.fp = open(self.filepath, 'at')

    def process_one(self, jd):
        """Dump json as string, each object per line."""
        try:
            line = json.dumps(jd)
        except TypeError as err:
            logger.error('JSON dumps: %s', err)
            return
        try:
            self.fp.write(line)
            self.fp.write('\n')
            self.fp.flush()
        except Exception as err:
            logger.error(err)
            if self.fp is not None:
                self.fp.close()
            raise

    def close(self):
        """Close the file."""
        self.fp.close()

    def __str__(self):
        """Give core information about this handler."""
        return 'FileHandler: path={}'.format(self.filepath)


class QueueHandler(BaseHandler):
    """A QueueHandler would implement a producer/consumer threads method to
    handle the streaming tweets. The main thread (producer) would put a tweet
    in a queue, and the folked thread (consumer) would parse and save this
    tweet into database.

    For the case of Twitter streaming, we prefer to parse the tweets one by
    one, but to save tweets into database in bulk at one time, in order to
    reduce the overhead as well as the number of database queries.

    To avoid the loss of tweets data because of temporal unvaialbe of database
    server. This class provides a function to cache the tweets in the queue
    for a certain period, e.g., 1 day. If the database server down time exceeds
    the threshold, we dump the unparsed tweets into a file and send an email
    to the administrator (if SMTPHandler is set in logging).
    """

    def __init__(self,
                 q,
                 dump_dir,
                 bucket_size=1000,
                 hold_on_failure=True,
                 hold_on_kwargs=None,
                 parser_kwargs=None):
        """Constructor of QueueHandler.

        Parameters
        ----------
        q : queue.Queue
            A queue instance.
        bucket_size : Integer
            Specify how many tweets should be saved in bulk.
        hold_on_failure : Boolean
            When database server is down, whether hold on. If false, the
            program will exist with exception.
        hold_on_kwargs : Dict
            Used when hold_on_failure is set to True. The default dictionary
            values are (time measured in second unit):
                sleep=60, sleep for 60 seconds before next db connection test;
                max=24*3600, hold for a maximum of one day before dumping to a
                    file. When database connection is recovered, the dumped
                    tweets would not automatically re-load into the database.
                    The administrator needs to take care of it.
        parser_kwargs : Dict
            The additional keywords to initialize Parser class. It is used in
            slaver thread, check its class defination.
        """
        # The sentinel to stop thread.
        self._sentinel = None
        self.queue = q
        self.dump_dir = dump_dir
        self.bucket_size = bucket_size
        self.bucket = []
        self.global_counter = 0
        # When PostgreSQL is down,
        # set hold_on = True, which will keep data for a maximum of 3 days
        # During hold-on, periodically try to reconnect server.
        self.hold_on_failure = hold_on_failure
        hold_on_kwargs = dict() if hold_on_kwargs is None else hold_on_kwargs
        self.hold_on_sleep_seconds = hold_on_kwargs.get('sleep', 60)
        self.hold_on_max_seconds = hold_on_kwargs.get('max', 24 * 3600)
        self.current_hold_on_seconds = 0
        self.is_connection_failed = False
        # keywords arguments when creating parser
        self.parser_kwargs = dict() if parser_kwargs is None else parser_kwargs
        # private members
        self._thread = None
        self._stop = threading.Event()
        # when server is down
        self._fp_db_down = None
        self._db_down_counter = 0
        # when bulk saving is failed
        self._fp_db_bulk_save = None

    def start(self):
        """Start the handler.

        This starts up a background thread to monitor the queue for
        tweet to process.
        """
        self._thread = t = threading.Thread(target=self._monitor)
        t.setDaemon(True)
        t.start()

    def stop(self):
        """Stop the listener.
        This asks the thread to terminate, and then waits for it to do so.
        Note that if you don't call this before your application exits, there
        may be some records still left on the queue, which won't be processed.
        """
        self._stop.set()
        self.enqueue_sentinel()
        self._thread.join()
        self._thread = None

    def restart(self):
        """Restart the listener."""
        if self._thread is not None:
            self.start()
        else:
            self.stop()
            self.start()

    def is_alive(self):
        """Test whether the consuming thread is alive"""
        return self._thread.is_alive()

    @staticmethod
    def test_db_connection(session):
        """Test whether the database connection resumed."""
        try:
            session.execute('select 1').fetchall()
            return True
        except SQLAlchemyError as err:
            logger.debug(err)
            session.rollback()
            return False

    def consume_this_bucket(self, parser, session, platform_id):
        """consume on bucket by parse and save all tweets in the bucket. """
        this_bucket_size = len(self.bucket)
        logger.info('Consumer thread: processed tweets N=%s',
                    self.global_counter)
        logger.info('Consumer thread: Parsing tweets one by one')
        parser.bulk_parse_and_save(
            self.bucket, session, platform_id, multiprocesses=False)

    def gen_filename(self, fn_pre, fn_ext):
        """Generate the filename when dumping"""
        filename = fn_pre + datetime.utcnow().strftime('%Y%m%d%H%M%S') + fn_ext
        return join(self.dump_dir, filename)

    def dump_bucket_to_file(self, fp):
        """Dump tweets in self.bucket into file """
        for jd in self.bucket:
            fp.write(json.dumps(jd))
            fp.write('\n')
        self.bucket = []

    def dump_queue_to_file(self, fp):
        """Dump current queued tweets into file. """
        counter = 0
        while True:
            try:
                jd = self.queue.get(block=False)
                if jd == self._sentinel:
                    return counter
                fp.write(json.dumps(jd))
                fp.write('\n')
                counter += 1
            except queue.Empty:
                return counter

    def on_db_server_down(self, session):
        """Handle the case when database server is down. """
        msg = 'Consumer thread: database server has been down for %s seconds'
        logger.warning(msg, self.current_hold_on_seconds)
        # option 1, do nothing and exit
        if self.hold_on_failure is False:
            msg = "Consumer thread: database server is down, exit!"
            logger.critical(msg)
            raise SystemExit(msg)
        # option 2, hold on the processing and wait for reconnection
        time.sleep(self.hold_on_sleep_seconds)
        self.current_hold_on_seconds += self.hold_on_sleep_seconds
        # if it reaches the maximum hold on seconds
        # current strategy is to dump tweets into file, including the queued
        if self.current_hold_on_seconds >= self.hold_on_max_seconds:
            msg = 'Consumer thread: timeout, dumping tweets ...'
            logger.warning(msg)
            if self._fp_db_down is None:
                filename = self.gen_filename(
                    fn_pre='dumped_tweets.db_down.', fn_ext='.json.gz')
                self._fp_db_down = gzip.open(filename, mode='at')
            # first dump the bucket
            msg = 'Consumer thread: dumping %s tweets in the bucket'
            if self.bucket:
                ntweets_dumped = len(self.bucket)
                self.dump_bucket_to_file(self._fp_db_down)
                logger.info(msg, ntweets_dumped)
                self._db_down_counter += ntweets_dumped
                self.global_counter += ntweets_dumped
                # empty the bucket
                self.bucket = []
            # then dump the queue
            ntweets_dumped = self.dump_queue_to_file(self._fp_db_down)
            msg = 'Consumer thread: dumping %s tweets in the queue'
            logger.info(msg, ntweets_dumped)
            self._db_down_counter += ntweets_dumped
            self.global_counter += ntweets_dumped
            msg = 'Consumer thread: dumping %s tweets in total'
            logger.info(msg, self._db_down_counter)
        logger.info('Consumer thread: re-connecting database server...')
        # if database reconnected
        msg = 'Consumer thread: database connection recovered after %s seconds'
        if self.test_db_connection(session) is True:
            logger.critical(msg, self.current_hold_on_seconds)
            logger.info('Consumer thread: dumping %s tweets in total',
                        self._db_down_counter)
            self.is_connection_failed = False
            self.current_hold_on_seconds = 0
            self._db_down_counter = 0
            if self._fp_db_down is not None:
                self._fp_db_down.close()
            self._fp_db_down = None

    def on_db_bulk_save_error(self):
        """When database error occurs on bulk saving operation, dump the
        tweets in self.bucket into file.
        """
        filename = self.gen_filename(
            fn_pre='dumped_tweets.db_down.', fn_ext='.json.txt')
        if self.bucket:
            self._fp_db_bulk_save = open(filename, mode='at')
            msg = 'Consumer thread: dumping %s tweets in the bucket'
            logger.info(msg, len(self.bucket))
            self.dump_bucket_to_file(self._fp_db_bulk_save)
            self.bucket = []
            self._fp_db_bulk_save.close()
            self._fp_db_bulk_save = None

    def _monitor(self):
        """Monitor the queue for tweet incoming and then parse and save it into
        the database.

        This method runs on a separate, internal thread.
        The thread will terminate if it sees a sentinel object in the queue.
        """
        # scoped_session
        # Session itself is not thread safe, we use scoped_session.
        # Each thread uses only one scoped_session object
        # We never delete anything from database in this function.
        # set expire_on_commit=False to avoid re-fetch of these existed objects
        session = Session(expire_on_commit=False)
        parser = Parser(**self.parser_kwargs)
        platform_id = get_platform_id(session, name=N_PLATFORM_TWITTER)
        has_task_done = hasattr(self.queue, 'task_done')
        while not self._stop.isSet():
            if self.is_connection_failed is True:
                self.on_db_server_down(session)
                continue
            # normal bulk insert process
            try:
                # fill the bucket
                for i in range(self.bucket_size):
                    # dequeue with block=True
                    jd = self.queue.get(True)
                    if has_task_done is True:
                        self.queue.task_done()
                    if jd is not self._sentinel:
                        self.global_counter += 1
                        self.bucket.append(jd)
                    else:
                        break
                # consume this bucket
                self.consume_this_bucket(parser, session, platform_id)
                self.bucket = []
            # database is shutdown unexpectedly
            except OperationalError as err:
                session.rollback()
                if 'server closed the connection unexpectedly' in repr(
                        err) or 'could not connect to server' in repr(err):
                    logger.critical('Causion: database server is down!')
                    self.is_connection_failed = True
                else:
                    logger.error(err)
                    self.on_db_bulk_save_error()
            except SQLAlchemyError as err:
                session.rollback()
                logger.exception(err)
                self.on_db_bulk_save_error()
            except BaseException as err:
                # unexpected exception, logging (will exit)
                logger.exception(err)
                raise
        # There might still be records in the queue.
        while True:
            try:
                jd = self.queue.get(False)
                if has_task_done:
                    self.queue.task_done()
                if jd is self._sentinel:
                    break
                self.bucket.append(jd)
            except queue.Empty:
                break
        if self.bucket:
            try:
                self.consume_this_bucket(parser, session, platform_id)
                self.bucket = []
            except SQLAlchemyError as err:
                session.rollback()
                logger.exception('Consumer thread: %s', err)
                self.on_db_bulk_save_error()
        if self._fp_db_down is not None:
            self._fp_db_down.close()
        if self._fp_db_bulk_save is not None:
            self._fp_db_bulk_save.close()

    def enqueue_sentinel(self):
        """This is used to enqueue the sentinel record.

        The base implementation uses put_nowait. You may want to override this
        method if you want to use timeouts or work with custom queue
        implementations.
        """
        self.queue.put_nowait(self._sentinel)

    # implement base class interface
    def process_one(self, jd):
        """Base interface that handler one tweet.

        In Queuehandler, just sent it into queue.
        """
        self.queue.put(jd)

    def close(self):
        self.stop()

    def __str__(self):
        return 'QueueHandler'
