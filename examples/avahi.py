#!/usr/bin/env python
#
# This file is part of python-tdbus. Python-tdbus is free software
# available under the terms of the MIT license. See the file "LICENSE" that
# was provided together with this source file for the licensing terms.
#
# Copyright (c) 2012 the python-tdbus authors. See the file "AUTHORS" for a
# complete list.

# This example shows how to access Avahi on the D-BUS.

import sys
from tdbus import *

CONN_AVAHI = 'org.freedesktop.Avahi'
PATH_SERVER = '/'
IFACE_SERVER = 'org.freedesktop.Avahi.Server'

conn = Connection(DBUS_BUS_SYSTEM)
dispatcher = BlockingDispatcher(conn)

try:
    result = dispatcher.call_method(PATH_SERVER, 'GetVersionString',
                        interface=IFACE_SERVER, destination=CONN_AVAHI)
except Error:
    print 'Avahi NOT available.'
    raise

print 'Avahi is available at %s' % CONN_AVAHI
print 'Avahi version: %s' % result[0]
print
print 'Browsing service types on domain: local'
print 'Press CTRL-\\ to exit'
print

result = dispatcher.call_method('/', 'ServiceTypeBrowserNew', interface=IFACE_SERVER,
                    destination=CONN_AVAHI, format='iisu', args=(-1, 0, 'local', 0))
browser = result[0]

def item_new(message, dispatcher):
    args = message.get_args()
    print 'service %s exists on domain %s' % (args[2], args[3])

dispatcher.add_signal_handler(browser, 'ItemNew', item_new)
dispatcher.dispatch()
