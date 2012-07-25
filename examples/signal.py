#!/usr/bin/env python
#
# This file is part of python-tdbus. Python-tdbus is free software
# available under the terms of the MIT license. See the file "LICENSE" that
# was provided together with this source file for the licensing terms.
#
# Copyright (c) 2012 the python-tdbus authors. See the file "AUTHORS" for a
# complete list.


# This example shows how to listen for signals. Here we listen for any signal
# but add_signal_handler() also accepts keyword arguments to only listen for
# specific signals.

import sys
from tdbus import *

conn = Connection(DBUS_BUS_SESSION)
dispatcher = BlockingDispatcher(conn)

def signal_handler(message, dispatcher):
    print 'signal received: %s, args = %s' % (message.get_member(), repr(message.get_args()))

dispatcher.add_signal_handler(callback=signal_handler)

print 'Listening for signals. Press CTRL-\\ to quit.'
print

dispatcher.dispatch()
