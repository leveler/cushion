import unittest
from base64 import b64encode, b64decode
from datetime import datetime, timedelta

import execjs

from ..cushion.model import Model, DocTypeMismatch, DocTypeNotFound
from ..cushion.field import (
    Field, BooleanField, TextField, IntegerField, FloatField, RefField,
    DateTimeField, ListField, DictField, ByteField, OptionField
    )

from ..cushion.persist import set_connection
from ..cushion.persist.mem import MemConnection


set_connection(MemConnection())


class Something(Model):
    twentythree = Field(default=23, loader=int)
    somestr = Field(loader=unicode)
    txt = TextField()
    i = IntegerField(default=37)
    f = FloatField()
    d = DateTimeField(default=datetime.utcnow)
    ll = ListField()
    dd = DictField()
    b = BooleanField()
    default_true = BooleanField(default=True)
    pic = ByteField()
    oo = OptionField(choices=['XYZ', 'ABC'])


class ModelWithNaiveDateTime(Model):
    d = DateTimeField(default=datetime.utcnow(), naive=True)


class Outter(Model):
    some = RefField(Something)


class TestField(unittest.TestCase):

    def test_no_type(self):
        notype = Something()
        assert "something" == notype.type

    def test_loader(self):
        s = Something()
        s.somestr = 'asdf'
        with self.assertRaises(Exception):
            s.twentythree = 'hexagonal alley'

    def test_byte_field(self):
        s = Something()
        sample = b64encode("SOME BINARY DATA I PROMISE")
        s.pic = sample
        s.save()
        s0 = Something.load(s.id)
        assert s0.pic == s.pic
        # can't assign None?
        s.pic = None
        s.save()

    def test_boolean_field(self):
        s = Something()
        assert not s.b, "not false by default?"
        s.b = True
        s.save()
        s0 = Something.load(s.id)
        assert s0.b, "why not true?"
        assert s0.default_true, "default true was not true"

    def test_option_field(self):
        s = Something()
        # can assign a valid option
        s.oo = 'XYZ'
        s.save()
        with self.assertRaises(ValueError):
            s.oo = 'feh'

    def test_integer_field(self):
        ss = Something(i=99)
        assert ss.i == 99, "{} != 99".format(ss.i)

    def test_float_field(self):
        ff = Something()
        ff.f = "2"
        assert ff.f == 2.0

    def test_ref_field(self):
        s = Something(txt='good times').save()
        o = Outter(some=s).save()
        assert o.some.txt == 'good times'
        o2 = Outter.load(o.id)
        assert o2.some.txt == 'good times'
        print o._raw_data
        assert o2.rawval('some') == s.id

    def test_datetime_field(self):
        s = Something().save()
        assert s.d < datetime.utcnow(), "date bogus"

    def test_datetime_field_is_naive(self):
        s = ModelWithNaiveDateTime().save()
        self.assertTrue(s.d.tzinfo == None)

    def test_datetime_field_is_not_naive(self):
        s = Something().save()
        self.assertFalse(s.d.tzinfo != None)

    def test_listfield(self):
        s = Something()
        s.ll.append('feh')
        s.save()
        assert len(s.ll)==1, "bogus len"
        s0 = Something.load(s.id)
        assert s.ll[0] == s0.ll[0]

    def test_dictfield(self):
        s = Something()
        s.dd['feh'] = 'foo'
        s.save()
        assert len(s.dd)==1, "bogus len"
        s0 = Something.load(s.id)
        assert s.dd['feh'] == s0.dd['feh']
