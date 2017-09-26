# -*- coding: utf-8 -*-
"""Set up database connection.

We use SQLAlchemy package to manage database.
"""
#
# written by Chengcheng Shao <sccotte@gmail.com>

from hoaxy import CONF
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
import os

# if you want to logging the actual queries, use the following setting.
# import logging
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

try:
    ENGINE = create_engine(
        URL(**CONF['database']['connect_args']),
        pool_size=CONF['database']['pool_size'],
        pool_recycle=CONF['database']['pool_recycle'],
        client_encoding='utf8')
    Session = scoped_session(sessionmaker(bind=ENGINE))
except Exception:
    raise


@event.listens_for(ENGINE, "connect")
def connect(dbapi_connection, connection_record):
    connection_record.info['pid'] = os.getpid()


@event.listens_for(ENGINE, "checkout")
def checkout(dbapi_connection, connection_record, connection_proxy):
    pid = os.getpid()
    if connection_record.info['pid'] != pid:
        connection_record.connection = connection_proxy.connection = None
        raise exc.DisconnectionError("Connection record belongs to pid %s, "
                                     "attempting to check out in pid %s" %
                                     (connection_record.info['pid'], pid))
