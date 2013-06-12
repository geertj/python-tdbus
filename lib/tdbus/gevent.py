#
# This file is part of python-tdbus. Python-tdbus is free software
# available under the terms of the MIT license. See the file "LICENSE" that
# was provided together with this source file for the licensing terms.
#
# Copyright (c) 2012 the python-tdbus authors. See the file "AUTHORS" for a
# complete list.

from __future__ import division, absolute_import

import gevent
from gevent import core, local
from gevent.hub import get_hub, Waiter

from tdbus import _tdbus
from tdbus.loop import EventLoop
from tdbus.connection import DBusConnection, DBusError


class GEventLoop(EventLoop):
    """Integration with the GEvent event loop."""

    def __init__(self, connection):
        self._connection = connection
        self._hub = get_hub()

    def add_watch(self, watch):
        fd = watch.get_fd()
        flags = watch.get_flags()
        evtype = 0
        if flags & _tdbus.DBUS_WATCH_READABLE:
            evtype |= core.READ
        if flags & _tdbus.DBUS_WATCH_WRITABLE:
            evtype |= core.WRITE
        event = get_hub().loop.io(fd, evtype)
        if watch.get_enabled():
            event.start(self._handle_watch, watch, pass_events=True)
        watch.set_data(event)

    def remove_watch(self, watch):
        event = watch.get_data()
        event.stop()
        watch.set_data(None)

    def watch_toggled(self, watch):
        event = watch.get_data()
        if watch.get_enabled():
            event.start(self._handle_watch, watch, pass_events=True)
        else:
            event.stop()

    def _handle_watch(self, evtype, watch):
        flags = 0
        if evtype & core.READ:
            flags |= _tdbus.DBUS_WATCH_READABLE
        if evtype & core.WRITE:
            flags |= _tdbus.DBUS_WATCH_WRITABLE
        watch.handle(flags)
        self._hub.loop.run_callback(self._handle_dispatch, self._connection)

    def add_timeout(self, timeout):
        interval = timeout.get_interval()
        event = get_hub().loop.timer(interval/1000, interval/1000)
        if timeout.get_enabled():
            event.start(self._handle_timeout, timeout)
        # Currently (June 2012) gevent does not support reading or changing
        # the interval of a timer. Libdbus however expects it an change the
        # interval, so we store it separately outside the event.
        timeout.set_data((interval,event))

    def remove_timeout(self, timeout):
        interval,event = timeout.get_data()
        event.stop()
        timeout.set_data(None)

    def timeout_toggled(self, timeout):
        interval,event = timeout.get_data()
        if timeout.get_enabled():
            if interval != timeout.get_interval():
                # Change interval => create new timer
                event.stop()
                event = get_hub().loop.timer(interval/1000, interval/1000)
                timeout.set_data(event)
            event.start(self._handle_timeout, timeout)
        else:
            event.stop()

    def _handle_timeout(self, timeout):
        timeout.handle()
        self._hub.loop.run_callback(self._handle_dispatch, self._connection)

    def _handle_dispatch(self, connection):
        while connection.get_dispatch_status() == _tdbus.DBUS_DISPATCH_DATA_REMAINS:
            connection.dispatch()


class GEventDBusConnection(DBusConnection):

    Loop = GEventLoop
    Local = local.local

    def call_method(self, *args, **kwargs):
        """Call a method."""
        callback = kwargs.get('callback')
        if callback is not None:
            super(GEventDBusConnection, self).call_method(*args, **kwargs)
            return
        waiter = Waiter()
        def _gevent_callback(message):
            waiter.switch(message)
        kwargs['callback'] = _gevent_callback
        super(GEventDBusConnection, self).call_method(*args, **kwargs)
        reply = waiter.get()
        if reply.get_type() == _tdbus.DBUS_MESSAGE_TYPE_ERROR:
            raise DBusError(reply.get_error_name())
        return reply

    def spawn(self, handler, *args):
        gevent.spawn(handler, *args)
