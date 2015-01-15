import unittest
from base64 import b64encode, b64decode
from datetime import datetime, timedelta

from ..cushion.model import Model, DocTypeMismatch, DocTypeNotFound
from ..cushion.field import (
    Field, BooleanField, TextField, IntegerField, FloatField, RefField,
    DateTimeField, ListField, DictField, ByteField
    )
from ..cushion.persist import set_connection, CouchbaseConnection


set_connection(CouchbaseConnection('lvlrtest', 'localhost', 'gogogogo'))


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
    pic = ByteField()


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
        sample = "SOME BINARY DATA I PROMISE"
        s.pic = sample
        s.save()
        s0 = Something.load(s.id)
        print "PIC  s0.pic", s0.pic, "s.pic", s.pic
        assert s0.pic == s.pic

    def test_boolean_field(self):
        s = Something()
        assert not s.b, "not false by default?"
        s.b = True
        s.save()
        s0 = Something.load(s.id)
        assert s0.b, "why not true?"

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

    def test_datetime_field(self):
        s = Something().save()
        assert s.d < datetime.utcnow(), "date bogus"

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


