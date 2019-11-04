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


class BaseHandler(object):
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
    # The sentinel to stop thread.
    _sentinel = None

    def __init__(self,
                 queue,
                 bucket_size=1000,
                 hold_on_failure=True,
                 hold_on_sleep_seconds=60,
                 hold_on_max_seconds=24 * 3600,
                 parser_kwargs=None,
                 bulk_save_kwargs=None):
        """Constructor of QueueHandler.

        Parameters
        ----------
        queue : queue
            A queue instance.
        bucket_size : Integer
            Specify how many tweets should be saved in bulk.
        hold_on_failure : Boolean
            When database server is down, whether hold on. If false, the
            program will exist with exception.
        hold_on_sleep_seconds : Integer
            The sleep time for the program before test database server again.
        hold_on_max_seconds : Integer
            The maximum hold on seconds. When exceeds we dump the tweets into
            a file. When database connection is recovered, the dumped tweets
            would not automatically re-load into the database. The
            administrator needs to take care of it.
        parser_kwargs : Dict
            The additional keywords to initialize Parser class. It is used in
            slaver thread.
        bulk_save_kwargs : Dict
            The additional keywords for Parser.bulk_save function. It is used
            in slaver thread.
        """
        self.queue = queue
        self.bucket_size = bucket_size
        self.bucket = []
        self.global_counter = 0
        # When PostgreSQL is down,
        # set hold_on = True, which will keep data for a maximum of 3 days
        # During hold-on, periodically try to reconnect server.
        self.hold_on_failure = hold_on_failure
        self.hold_on_sleep_seconds = hold_on_sleep_seconds
        self.hold_on_max_seconds = hold_on_max_seconds
        self.current_hold_on_seconds = 0
        self.is_connection_failed = False
        # keywords arguments when creating parser
        self.parser_kwargs = dict() if parser_kwargs is None else parser_kwargs
        self.bulk_save_kwargs = dict(
        ) if bulk_save_kwargs is None else bulk_save_kwargs
        # private members
        self._thread = None
        self._stop = threading.Event()
        # when server is down
        self._fp_db_down = None
        # when bulk saving is failed
        self._fp_db_bulk_save = None
        # send mail only once
        self._is_critical_mailed = False

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

    def test_db_connection(self, session):
        """Test whether the database connection resumed."""
        try:
            session.execute('select 1').fetchall()
            return True
        except SQLAlchemyError as e:
            logger.info(e)
            session.rollback()
            return False
        except Exception as e:
            logger.error(e)
            return False

    def _consume_this_bucket(self, parser, session, platform_id):
        """consume on bucket by parse and save all tweets in the bucket.
        """
        this_bucket_size = len(self.bucket)
        logger.info('Global tweets counter is %s', self.global_counter)
        logger.info('Current qsize is %s', self.queue.qsize())
        logger.info('Current bucket size is %s', this_bucket_size)
        logger.info('Trying to parsing tweets one by one')
        parsed_results = parser.parse_many(self.bucket, multiprocesses=False)
        dfs = parser.to_dict(parsed_results)
        parser.bulk_save(session, dfs, platform_id)

    def _dump_queue_to_file(self):
        """dump current queued tweets into file.
        """
        logger.info('Dumping %s tweets in bucket buffer.', len(self.bucket))
        if len(self.bucket) > 0:
            for jd in self.bucket:
                self._fp_db_down.write(json.dumps(jd))
                self._fp_db_down.write('\n')
            self.bucket = []
        logger.info('Dumping tweets in the queue')
        while True:
            try:
                jd = self.queue.get(block=False)
                if jd == self._sentinel:
                    logger.info('Stop sign recevied, exit!')
                    self._fp_db_down.close()
                    return
                self._fp_db_down.write(json.dumps(jd))
                self._fp_db_down.write('\n')
            except queue.Empty:
                break

    def _on_db_server_down(self, session):
        """Handle the case when dabase server is down.
        """
        logger.info('Elapsed about %s seconds, since last server down',
                    self.current_hold_on_seconds)
        # option 1, do nothing and exit
        if self.hold_on_failure is False:
            msg = "The database server is down, exit!"
            logger.critical(msg)
            raise SystemExit(msg)
        # option 2, hold on the processing and wait for reconnection
        time.sleep(self.hold_on_sleep_seconds)
        self.current_hold_on_seconds += self.hold_on_sleep_seconds
        # if it reaches the maximum hold on seconds
        # current strategy exit the handler
        if self.current_hold_on_seconds >= self.hold_on_max_seconds:
            logger.warning('Hold on time exceeds threshold, dumping tweets..')
            if self._fp_db_down is None:
                filename = 'dumped_tweets.db_server_down.' + \
                    datetime.utcnow().strftime('%Y%m%d%H%M%S') + '.json.txt'
                self._fp_db_down = open(filename, mode='a')
            self._dump_queue_to_file()
        logger.info('Hold on, keep tring to connect SQL server...')
        # if database reconnected
        if self.test_db_connection(session) is True:
            self.is_connection_failed = False
            self.current_hold_on_seconds = 0
            self._fp_db_down.close()
            self._fp_db_down = None
            self._is_critical_mailed = False

    def _on_db_bulk_save_error(self):
        """ When database error occurs on bulk saving operation, dump the
        tweets in self.bucket into file.
        """
        filename = 'dumped_tweets.db_bulk_save.' + datetime.utcnow().strftime(
            '%Y%m%d%H%M%S') + '.json.txt'
        self._fp_db_bulk_save = gzip.GzipFile(filename, mode='a')
        for jd in self.bucket:
            self._fp_db_bulk_save.write(json.dumps(jd))
            self._fp_db_bulk_save.write('\n')
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
                self._on_db_server_down(session)
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
                self._consume_this_bucket(parser, session, platform_id)
                self.bucket = []
                logger.info('Operations on this bucket is done')
            except queue.Empty:
                break
            except Exception as e:
                logger.error('Exception %s when bulk parsing and saving', e)
                if isinstance(e, SQLAlchemyError):
                    session.rollback()
                    if isinstance(e, OperationalError):
                        # if 'could not connect to server' in str(e):
                        if self._is_critical_mailed is False:
                            self._is_critical_mailed = True
                            logger.critical('Database server is down!')
                        logger.error('Hold on until SQL service back! %s', e)
                        self.is_connection_failed = True
                    else:
                        logger.error('Exception happedn when bulk saving')
                        self._on_db_bulk_save_error()
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
        try:
            if len(self.bucket) > 0:
                self._consume_this_bucket(parser, session, platform_id)
                self.bucket = []
        except Exception as e:
            logger.error('Exception %s when parsing %s', e, jd)
            if isinstance(e, SQLAlchemyError):
                session.rollback()
                self._on_db_bulk_save_error()

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
