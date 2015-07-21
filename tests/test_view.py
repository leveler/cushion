import unittest
from datetime import datetime, timedelta

from ..cushion.model import Model, DocTypeMismatch, DocTypeNotFound
from ..cushion.field import (
    Field, TextField, IntegerField, FloatField, RefField, DateTimeField
    )
from ..cushion.persist import set_connection, Persist
from ..cushion.persist.mem import MemConnection
from ..cushion.view import View, sync_all


class Boogie(Model):
    n = TextField()
    i = IntegerField(default=37)
    f = FloatField()
    d = DateTimeField(default=datetime.utcnow)

    all_docs = View(
        'boog', 'allthem',
        '''
        function(doc, meta) {
            emit(meta.id)
        }
        ''' )

    by_n = View(
        'boog', 'by_n',
        '''
        function(doc) {
            if (doc.type == "boogie") {
                emit(doc.n, null)
            }
        }
        ''' )

    by_composite_key = View(
        'boog', 'by_comp',
        '''
        function(doc) {
            if (doc.type == "boogie") {
                emit([doc.n, doc.i], null)
            }
        }
        ''' )


def clean_out_db_docs():
    for d in Boogie.all_docs(wrapper=None):
        Persist().delete(d.docid)


class Outter(Model):
    some = TextField()


class TestField(unittest.TestCase):

    def setUp(self):
        set_connection(MemConnection())
        sync_all(Boogie.viewlist())
        clean_out_db_docs()

    def test_by_n(self):
        b0 = Boogie(n='one').save()
        b1 = Boogie(n='two').save()
        res = Boogie.by_n(stale=False, startkey=b0.n, endkey=b0.n,
                include_docs=True)
        r = res[0]
        assert isinstance(r, Boogie), "not a Boogie"
        assert r.n == b0.n
        assert r.id == b0.id

    def test_by_comp(self):
        b0 = Boogie(n='one').save()
        b1 = Boogie(n='two').save()
        res = Boogie.by_composite_key(startkey=[b0.n, 37], endkey=[b0.n, 37],
                include_docs=True)
        r = res[0]
        assert isinstance(r, Boogie), "not a Boogie"
        assert r.n == b0.n
        assert r.id == b0.id

    def test_descend(self):
        b0 = Boogie(n='one').save()
        b1 = Boogie(n='two').save()
        Boogie(n='z0').save()
        Boogie(n='z2').save()
        Boogie(n='z8').save()
        Boogie(n='z9').save()
        res = Boogie.by_composite_key(
                startkey=['z9', 37], endkey=['tw', 37],
                descending=True, include_docs=True)
        self.assertEqual(len(res), 5)
        r = res[0] # should be z9
        assert 'z9' == r.n


