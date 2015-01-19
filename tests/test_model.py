import unittest

from ..cushion.model import Model, DocTypeMismatch, DocTypeNotFound
from ..cushion.field import Field
from ..cushion.persist import set_connection, CouchbaseConnection


set_connection(CouchbaseConnection('lvlrtest', 'localhost', 'gogogogo'))


class NoTypeModel(Model):
    pass


class FakeModel(Model):
    @property
    def type(self):
        return "fake"

    twentythree = Field(default=23)
    somestr = Field()


class TestModel(unittest.TestCase):

    def test_no_type(self):
        notype = NoTypeModel()
        assert "notypemodel" == notype.type

    def test_save(self):
        f = FakeModel()
        f.save()

    def test_load(self):
        f = FakeModel().save()
        f2 = FakeModel.load(f.id)
        assert f.id == f2.id


