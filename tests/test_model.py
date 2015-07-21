import unittest

from ..cushion.model import Model, DocTypeMismatch, DocTypeNotFound
from ..cushion.field import Field, TextField
from ..cushion.persist import set_connection, get_connection
from ..cushion.persist.mem import MemConnection





class NoTypeModel(Model):
    pass


class FakeModel(Model):

    @property
    def type(self):
        return "fake"

    twentythree = Field(default=23)
    somestr = Field()
    txt = TextField()
    default_val = TextField(default='aaa')


class TestModel(unittest.TestCase):

    def setUp(self):
        set_connection(MemConnection())

    def test_no_type(self):
        notype = NoTypeModel()
        assert "notypemodel" == notype.type

    def test_save(self):
        f = FakeModel()
        f.save()

    def test_load_fail(self):
        assert None == FakeModel.load('nope')

    def test_load(self):
        f = FakeModel().save()
        f2 = FakeModel.load(f.id)
        assert f.id == f2.id

    def test_eq(self):
        e1 = FakeModel().save()
        e2 = FakeModel.load(e1.id)
        e3 = FakeModel().save()
        assert e1 == e2, "not equal"
        assert not e1 == e3, "equal but shouldn't be"

    def test_raw(self):
        f = FakeModel(txt=55).save()
        self.assertEqual( 55, f.rawval('txt') )
        self.assertEqual( '55', f.txt )

    def test_unassigned_default(self):
        f = FakeModel().save()
        d = get_connection().get(f.id)
        self.assertEqual(d['default_val'], 'aaa')

