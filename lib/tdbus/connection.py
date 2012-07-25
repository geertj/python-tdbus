#
# This file is part of python-tdbus. Python-tdbus is free software
# available under the terms of the MIT license. See the file "LICENSE" that
# was provided together with this source file for the licensing terms.
#
# Copyright (c) 2012 the python-tdbus authors. See the file "AUTHORS" for a
# complete list.

import sys
import logging
import traceback
from tdbus import _tdbus

DBusError = _tdbus.Error


class DBusConnection(object):
    """A connection to the D-BUS."""

    Loop = None

    def __init__(self, address):
        """Create a new connection."""
        if self.Loop is None:
            raise NotImplementedError('cannot create Connection without a Loop')
        self._connection = _tdbus.Connection(address)
        self._connection.set_loop(self.Loop(self._connection))
        self._connection.add_filter(self._dispatch)
        self.handlers = []
        self.logger = logging.getLogger('tdbus')

    def add_handler(self, handler):
        """Add a new method/signal handler for this connection."""
        self.handlers.append(handler)

    def open(self, address):
        self._connection.open(address)

    def close(self):
        """Close the connection."""
        self._connection.close()

    def get_unique_name(self):
        """Return the unique connection name."""
        return self._connection.get_unique_name()

    def send_method_return(self, message, format=None, args=None):
        """Send a method call return."""
        reply = _tdbus.Message(_tdbus.DBUS_MESSAGE_TYPE_METHOD_RETURN,
                               reply_serial=message.get_serial(),
                               destination=message.get_sender())
        if format is not None:
            reply.set_args(format, args)
        self._connection.send(reply)

    def send_error(self, message, error_name, format=None, args=None):
        """Send an error reply."""
        reply = _tdbus.Message(_tbus.DBUS_MESSAGE_TYPE_ERROR,
                               reply_serial=message.get_serial(),
                               destination=message.get_sender(),
                               error_name=error_name)
        if format is not None:
            reply.set_args(format, args)
        self._connection.send(reply)

    def send_signal(self, path, member, interface=None, format=None, args=None,
                    destination=None):
        """Send a signal."""
        if '.' in member:
            member, interface = self._split_member(member)
        if interface is None:
            raise Error('you need to specify the interface')
        message = _tdbus.Message(_tdbus.DBUS_MESSAGE_TYPE_SIGNAL,
                                 member=member, interface=interface, path=path)
        if destination is not None:
            message.set_destination(destination)
        if format is not None:
            message.set_args(format, args)
        self._connection.send(message)

    def _dispatch(self, message):
        """Dispatch a message. This is installed as a filter into libdbus so
        it gets called on all incoming messages."""
        for handler in self.handlers:
            self.spawn(handler.dispatch, self, message)
        return True

    def spawn(self, handler, *args):
        """Spawn a handler. Can be overrided in a subclass."""
        try:
            handler(*args)
        except Exception as e:
            lines = ['Uncaught exception in method call']
            lines += traceback.format_exception(*sys.exc_info())
            for line in lines:
                self.logger.error(line)

    def call_method(self, path, member, interface=None, format=None, args=None,
                    destination=None, callback=None, timeout=None):
        """Call a method."""
        message = _tdbus.Message(_tdbus.DBUS_MESSAGE_TYPE_METHOD_CALL,
                                 path=path, member=member, interface=interface,
                                 destination=destination)
        if format is not None:
            message.set_args(format, args)
        if callback is None:
            message.set_no_reply(True)
            self._connection.send(message)
        else:
            if timeout is None:
                timeout = -1
            else:
                timeout = int(1000 * timeout)
            deferred = self._connection.send_with_reply(message, timeout)
            deferred.set_notify(callback)
