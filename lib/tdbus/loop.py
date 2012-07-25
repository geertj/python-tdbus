#
# This file is part of python-tdbus. Python-tdbus is free software
# available under the terms of the MIT license. See the file "LICENSE" that
# was provided together with this source file for the licensing terms.
#
# Copyright (c) 2012 the python-tdbus authors. See the file "AUTHORS" for a
# complete list.


class EventLoop(object):
    """Base class for event loops. You need an instance of this
    class to work with python-tdbus."""


    def add_watch(self, watch):
        raise NotImplementedError

    def remove_watch(self, watch):
        raise NotImplementedError

    def watch_toggled(self, watch):
        raise NotImplementedError

    def add_timeout(self, timeout):
        raise NotImplementedError

    def remove_timeout(self, timeout):
        raise NotImplementedError

    def timeout_toggled(self, timeout):
        raise NotImplementedError
