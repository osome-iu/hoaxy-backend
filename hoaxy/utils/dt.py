# -*- coding: utf-8 -*-
"""DATATIME functions."""
#
# written by Chengcheng Shao <sccotte@gmail.com>

from datetime import datetime
import dateutil.parser
import dateutil.relativedelta
import logging
import pytz

logger = logging.getLogger(__name__)
epoch = datetime.utcfromtimestamp(0)


def unix_timestamps_int(dt):
    """Return seconds from epoch to dt."""
    return int((dt - epoch).total_seconds())


def datetime_strformat(dt_str,
                       strf='%Y-%m-%d %H:%M:%S',
                       tz=pytz.UTC,
                       default=datetime(2000, 1, 1)):
    """Convert datetime string into another datetime string.

    Transform a datetime string to another datetime string
    Supported input datetime string formate are:
        Thu Jan 07 05:34:11 +0000 2016, tweet
        2014-10-03T19:57:38+00:00, many news site

    If we cannot parse, the original string is returned.
    """
    if dt_str:
        try:
            dt = dateutil.parser.parse(dt_str, default=default)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=pytz.UTC)
            return dt.astimezone(tz).strftime(strf)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            logger.exception("Exception when parsing datetime string: %s",
                             dt_str)
            return None


def utc_from_str(dt_str, with_tzinfo=False):
    """Convert datetime str to datetime (utc)."""
    try:
        dt = dateutil.parser.parse(dt_str, default=datetime(1970, 1, 1))
        if dt.tzinfo:
            dt = dt.astimezone(pytz.UTC)
            if with_tzinfo is False:
                return dt.replace(tzinfo=None)
            else:
                return dt
        else:
            if with_tzinfo is False:
                return dt.replace(tzinfo=None)
            else:
                return dt.replace(tzinfo=pytz.UTC)
    except (ValueError, OverflowError) as e:
        logger.error('Error when parsing datetime string %s: %s', dt_str, e)
        return None


def utc_from_seconds(seconds):
    """Convert unix timestampe (in seconds) to datetime (utc)."""
    try:
        dt = datetime.utcfromtimestamp(seconds)
        return dt
    except ValueError as e:
        logger.error('Error when paring timestamp %s: %s', seconds, e)
        return None


def to_utc_strformat(str_or_seconds, strf='%Y-%m-%d %H:%M:%S'):
    """Convert datetime string or unix timestamp to another datetime string."""
    if isinstance(str_or_seconds, str):
        dt = utc_from_str(str_or_seconds)
    else:
        dt = utc_from_seconds(str_or_seconds)
    if dt:
        return dt.strftime(strf)
