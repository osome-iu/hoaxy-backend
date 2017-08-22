# -*- coding: utf-8 -*-
"""Hoaxy exception moduel."""

#
# written by Chengcheng Shao <sccotte@gmail.com>


class HoaxyError(Exception):
    """The base class of all other hoaxy exception class.

    HoaxyError derived from build-in exception class Exception.
    """

    def __init__(self, *args, **kwargs):
        super(HoaxyError, self).__init__(*args, **kwargs)


class HoaxyDBConcurrency(HoaxyError):
    """Database insert concurrency error."""
    pass


# Commands
class CmdUsageError(Exception):
    """To indicate a command-line usage error"""

    def __init__(self, *args, **kwargs):
        self.print_help = kwargs.pop('print_help', True)
        super(CmdUsageError, self).__init__(*args, **kwargs)


class ArgumentError(Exception):
    """To indicate Argument Error"""

    def __init__(self, *args, **kwargs):
        super(ArgumentError, self).__init__(*args, **kwargs)


class APIError(HoaxyError):
    """Indicate APIError."""

    def __init__(self, *args, **kwargs):
        super(APIError, self).__init__(*args, **kwargs)


class APINoResultError(APIError):
    """Indicate no result found."""

    def __init__(self, *args, **kwargs):
        super(APINoResultError, self).__init__(*args, **kwargs)


class APIParseError(APIError):
    """Indicate API parse input error."""

    def __init__(self, *args, **kwargs):
        super(APIParseError, self).__init__(*args, **kwargs)
