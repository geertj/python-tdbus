#
# This file is part of python-tdbus. Python-tdbus is free software
# available under the terms of the MIT license. See the file "LICENSE" that
# was provided together with this source file for the licensing terms.
#
# Copyright (c) 2012 the python-tdbus authors. See the file "AUTHORS" for a
# complete list.

import sys
import logging
import fnmatch
import traceback

from tdbus import _tdbus, DBusError


def method(path=None, member=None, interface=None):
    def _decorate(func):
        func.method = True
        func.member = member or func.__name__
        func.path = path
        func.interface = interface
        return func
    return _decorate
 
def signal_handler(path=None, member=None, interface=None):
    def _decorate(func):
        func.signal_handler = True
        func.member = member or func.__name__
        func.path = path
        func.interface = interface
        return func
    return _decorate


class DBusHandler(object):
    """Handler for method calls and signals."""

    def __init__(self):
        self.methods = {}
        self.signal_handlers = {}
        self.logger = logging.getLogger('tdbus')
        self._init_handlers()

    def _init_handlers(self):
        for name in vars(self.__class__):
            handler = getattr(self, name)
            if getattr(handler, 'method', False):
                self.methods[handler.member] = handler
            elif getattr(handler, 'signal_handler', False):
                self.signal_handlers[handler.member] = handler

    def _get_connection(self):
        return self.local.connection

    connection = property(_get_connection)

    def _get_message(self):
        return self.local.message

    message = property(_get_message)

    def set_response(self, format, args):
        """Used by method call handlers to set the response arguments."""
        self.local.response = (format, args)

    def dispatch(self, connection, message):
        """Dispatch a message. Returns True if the message was dispatched."""
        if not hasattr(self, 'local'):
            self.local = connection.Local()
        self.local.connection = connection
        self.local.message = message
        self.local.response = (None, None)
        mtype = message.get_type()
        member = message.get_member()
        if mtype == _tdbus.DBUS_MESSAGE_TYPE_METHOD_CALL:
            if member not in self.methods:
                return False
            handler = self.methods[member]
            if handler.interface and handler.interface != message.get_interface():
                return False
            if handler.path and not fnmatch.fnmatch(message.get_path(), handler.path):
                return False
            try:
                ret = handler(message)
            except DBusError as e:
                self.connection.send_error(message, e[0])
            except Exception as e:
                lines = ['Uncaught exception in method call']
                lines += traceback.format_exception(*sys.exc_info())
                for line in lines:
                    self.logger.error(line)
                self.connection.send_error(message, 'UncaughtException')
            else:
                fmt, args = self.local.response
                self.connection.send_method_return(message, fmt, args)
        elif mtype == _tdbus.DBUS_MESSAGE_TYPE_SIGNAL:
            if member not in self.signal_handlers:
                return False
            handler = self.signal_handlers[member]
            if handler.interface and handler.interface != message.get_interface():
                return False
            if handler.path and not fnmatch.fnmatch(message.get_path(), handler.path):
                return False
            try:
                ret = handler(message)
            except Exception as e:
                lines = ['Uncaught exception in signal handler']
                lines += traceback.format_exception(*sys.exc_info())
                for line in lines:
                    self.logger.error(line)
        else:
            return False
