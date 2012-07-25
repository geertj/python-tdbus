#!/usr/bin/env python
#
# This file is part of python-tdbus. Python-tdbus is free software
# available under the terms of the MIT license. See the file "LICENSE" that
# was provided together with this source file for the licensing terms.
#
# Copyright (c) 2012 the python-tdbus authors. See the file "AUTHORS" for a
# complete list.

# This example shows how to handle a method call.

from tdbus import *

conn = Connection(DBUS_BUS_SESSION)
dispatcher = BlockingDispatcher(conn)

def hello_handler(message, dispatcher):
    hello = 'Hello, world!'
    print 'receive a Hello request, responding with "%s"' % hello
    dispatcher.send_method_return(message, 's', (hello,))

dispatcher.add_method_handler('/', 'Hello', hello_handler)

print 'Exposing a hello world method on the bus.'
print 'In another terminal, issue:'
print
print '  $ dbus-send --session --print-reply --dest=%s / com.example.Hello' % conn.get_unique_name()
print
print 'Press CTRL-\\ to exit.'
print
dispatcher.dispatch()
