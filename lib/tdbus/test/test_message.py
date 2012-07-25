#
# This file is part of python-tdbus. Python-tdbus is free software
# available under the terms of the MIT license. See the file "LICENSE" that
# was provided together with this source file for the licensing terms.
#
# Copyright (c) 2012 the python-tdbus authors. See the file "AUTHORS" for a
# complete list.

import math
import time
from threading import Thread, currentThread

import tdbus
from tdbus import *
from tdbus.test.base import BaseTest
from nose.tools import assert_raises

IFACE_EXAMPLE = 'com.example'


class MessageTest(BaseTest):

    def test_arg_byte(self):
        assert self.echo('y', (0,)) == (0,)
        assert self.echo('y', (10,)) == (10,)
        assert self.echo('y', (0xff,)) == (0xff,)

    def test_arg_byte_out_of_range(self):
        assert_raises(DBusError, self.echo, 'y', (-1,))
        assert_raises(DBusError, self.echo, 'y', (0x100,))

    def test_arg_int16(self):
        assert self.echo('n', (-0x8000,)) == (-0x8000,)
        assert self.echo('n', (-10,)) == (-10,)
        assert self.echo('n', (-0,)) == (-0,)
        assert self.echo('n', (10,)) == (10,)
        assert self.echo('n', (0x7fff,)) == (0x7fff,)

    def test_arg_int16_out_of_range(self):
        assert_raises(DBusError, self.echo, 'n', (-0x8001,))
        assert_raises(DBusError, self.echo, 'n', (0x8000,))

    def test_arg_uint16(self):
        assert self.echo('q', (0,)) == (0,)
        assert self.echo('q', (10,)) == (10,)
        assert self.echo('q', (0xffff,)) == (0xffff,)

    def test_arg_uint16_out_of_range(self):
        assert_raises(DBusError, self.echo, 'q', (-1,))
        assert_raises(DBusError, self.echo, 'q', (0x10000,))

    def test_arg_int32(self):
        assert self.echo('i', (-0x80000000,)) == (-0x80000000,)
        assert self.echo('i', (-10,)) == (-10,)
        assert self.echo('i', (0,)) == (0,)
        assert self.echo('i', (10,)) == (10,)
        assert self.echo('i', (0x7fffffff,)) == (0x7fffffff,)

    def test_arg_int32_out_of_range(self):
        assert_raises(DBusError, self.echo, 'i', (-0x80000001,))
        assert_raises(DBusError, self.echo, 'i', (0x80000000,))

    def test_arg_uint32(self):
        assert self.echo('u', (0,)) == (0,)
        assert self.echo('u', (10,)) == (10,)
        assert self.echo('u', (0xffffffff,)) == (0xffffffff,)

    def test_arg_uint32_out_of_range(self):
        assert_raises(DBusError, self.echo, 'u', (-1,))
        assert_raises(DBusError, self.echo, 'q', (0x100000000,))

    def test_arg_int64(self):
        assert self.echo('x', (-0x8000000000000000,)) == (-0x8000000000000000,)
        assert self.echo('x', (-10,)) == (-10,)
        assert self.echo('x', (0,)) == (0,)
        assert self.echo('x', (10,)) == (10,)
        assert self.echo('x', (0x7fffffffffffffff,)) == (0x7fffffffffffffff,)

    def test_arg_int64_out_of_range(self):
        assert_raises(DBusError, self.echo, 'x', (-0x8000000000000001,))
        assert_raises(DBusError, self.echo, 'x', (0x8000000000000000,))

    def test_arg_uint64(self):
        assert self.echo('t', (0,)) == (0,)
        assert self.echo('t', (10,)) == (10,)
        assert self.echo('t', (0xffffffffffffffff,)) == (0xffffffffffffffff,)

    def test_arg_uint64_out_of_range(self):
        assert_raises(DBusError, self.echo, 't', (-1,))
        assert_raises(DBusError, self.echo, 't', (0x10000000000000000,))

    def test_arg_boolean(self):
        assert self.echo('y', (False,)) == (False,)
        assert self.echo('y', (True,)) == (True,)
        assert self.echo('y', (0,)) == (False,)
        assert self.echo('y', (1,)) == (True,)

    def test_arg_double(self):
        assert self.echo('d', (-1e100,)) == (-1e100,)
        assert self.echo('d', (-1.0,)) == (-1.0,)
        assert self.echo('d', (-1e-100,)) == (-1e-100,)
        assert self.echo('d', (0.0,)) == (0.0,)
        assert self.echo('d', (1e-100,)) == (1e-100,)
        assert self.echo('d', (1.0,)) == (1.0,)
        assert self.echo('d', (1e100,)) == (1e100,)

    def test_arg_double_special(self):
        inf = 1e1000
        assert self.echo('d', (inf,)) == (inf,)
        assert self.echo('d', (-inf,)) == (-inf,)
        assert self.echo('d', (1/inf,)) == (1/inf,)  # 0
        assert self.echo('d', (1/-inf,)) == (1/-inf,)  # -0
        nan = inf/inf
        assert math.isnan(self.echo('d', (nan,))[0])  # note: nan != nan

    def test_arg_string(self):
        assert self.echo('s', ('',)) == ('',)
        assert self.echo('s', ('foo',)) == ('foo',)

    def test_arg_string_unicode(self):
        assert self.echo('s', (u'foo \u20ac',)) == (u'foo \u20ac',)

    def test_arg_object_path(self):
        assert self.echo('o', ('/foo/bar',)) == ('/foo/bar',)

    def test_arg_invalid_object_path(self):
        assert_raises(DBusError, self.echo, 'o', ('foo',))
        assert_raises(DBusError, self.echo, 'o', ('foo/bar',))
        assert_raises(DBusError, self.echo, 'o', ('/foo/bar/',))
        assert_raises(DBusError, self.echo, 'o', ('/foo//bar/',))
        assert_raises(DBusError, self.echo, 'o', ('/foo bar/',))

    def test_arg_signature(self):
        assert self.echo('g', ('iii',)) == ('iii',)

    def test_arg_invalid_signature(self):
        assert_raises(DBusError, self.echo, '*', (1,))
        assert_raises(DBusError, self.echo, '(i', (1,))
        assert_raises(DBusError, self.echo, 'i' * 256, (1,)*256)
        def nested_tuple(d,v):
            if d == 0:
                return (v,)
            return (nested_tuple(d-1,v),)
        assert_raises(DBusError, self.echo, '('*33 + 'i' + ')'*33,
                      nested_tuple(33,1))
        assert_raises(DBusError, self.echo, 'a'*33+'i', nested_tuple(33,1))

    def test_arg_variant(self):
        assert self.echo('v', (('i', 10),)) == (('i', 10),)
        assert self.echo('v', (('ai', [1,2,3]),)) == (('ai', [1,2,3]),)

    def test_arg_invalid_variant(self):
        assert_raises(DBusError, self.echo, 'v', (('ii', (1,2)),))

    def test_arg_multi(self):
        assert self.echo('ii', (1, 2)) == (1, 2)
        assert self.echo('iii', (1, 2, 3)) == (1, 2, 3)

    def test_arg_too_few(self):
        assert_raises(DBusError, self.echo, 'ii', (1,))

    def test_arg_too_many(self):
        assert_raises(DBusError, self.echo, 'ii', (1,2,3))

    def test_arg_struct(self):
        assert self.echo('(i)', ((1,),)) == ((1,),)
        assert self.echo('(ii)', ((1, 2),)) == ((1, 2),)
        assert self.echo('(iii)', ((1, 2, 3),)) == ((1, 2, 3),)
        assert self.echo('(((((i)))))', ((((((1,),),),),),)) == \
                    ((((((1,),),),),),)

    def test_arg_invalid_struct(self):
        assert_raises(DBusError, self.echo, '(i', ((10,),))
        assert_raises(DBusError, self.echo, '(i}', ((10,),))

    def test_arg_array(self):
        assert self.echo('ai', ([1],)) == ([1],)
        assert self.echo('ai', ([1,2],)) == ([1,2],)
        assert self.echo('ai', ([1,2,3],)) == ([1,2,3],)
        assert self.echo('a(ii)', ([(1,2), (3,4)],)) == ([(1,2),(3,4)],)
        assert self.echo('av', ([('i',10),('s','foo')],)) == \
                    ([('i',10),('s','foo')],)

    def test_arg_dict(self):
        assert self.echo('a{ss}', ({'foo': 'bar'},)) == ({'foo': 'bar'},)
        assert self.echo('a{ss}', ({'foo': 'bar', 'baz': 'qux'},)) == \
                    ({'foo': 'bar', 'baz': 'qux'},)
        assert self.echo('a{si}', ({'foo': 10},)) == ({'foo': 10},)
        assert self.echo('a{ii}', ({1: 10},)) == ({1: 10},)

    def test_arg_byte_array(self):
        assert self.echo('ay', ('foo',)) == ('foo',)

    def test_arg_byte_array_illegal_type(self):
        assert_raises(DBusError, self.echo, 'ay', ([1,2,3],))


class EchoHandler(DBusHandler):

    @method(interface=IFACE_EXAMPLE)
    def Echo(self, *args):
        self.set_response(self.message.get_signature(), self.message.get_args())

    @method(interface=IFACE_EXAMPLE)
    def Stop(self, *args):
        self.connection.stop()


class TestMessageSimple(MessageTest):

    @classmethod
    def dbus_server(cls, conn):
        conn.dispatch()

    @classmethod
    def setup_class(cls):
        super(TestMessageSimple, cls).setup_class()
        handler = EchoHandler()
        conn = SimpleDBusConnection(DBUS_BUS_SESSION)
        conn.add_handler(handler)
        cls.server_name = conn.get_unique_name()
        cls.server = Thread(target=cls.dbus_server, args=(conn,))
        cls.server.start()
        cls.client = SimpleDBusConnection(DBUS_BUS_SESSION)

    @classmethod
    def teardown_class(cls):
        cls.client.call_method('/', 'Stop', IFACE_EXAMPLE, destination=cls.server_name)
        cls.server.join()
        super(TestMessageSimple, cls).teardown_class()

    @classmethod
    def echo(cls, format=None, args=None):
        reply = cls.client.call_method('/', 'Echo', IFACE_EXAMPLE, format, args,
                                       destination=cls.server_name, timeout=10)
        return reply.get_args()


class TestMessageGEvent(MessageTest):

    @classmethod
    def setup_class(cls):
        super(TestMessageGEvent, cls).setup_class()
        handler = EchoHandler()
        conn = GEventDBusConnection(DBUS_BUS_SESSION)
        conn.add_handler(handler)
        cls.server_name = conn.get_unique_name()
        cls.client = GEventDBusConnection(DBUS_BUS_SESSION)

    @classmethod
    def echo(cls, format=None, args=None):
        reply = cls.client.call_method('/', 'Echo', IFACE_EXAMPLE, format, args,
                                       destination=cls.server_name, timeout=10)
        return reply.get_args()
