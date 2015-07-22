
from json import dumps, loads
from textwrap import dedent
from uuid import uuid4

from couchbase.bucket import Bucket
from couchbase.views.iterator import View

from .base import BaseConnection
from .exceptions import PersistenceError


class CouchbaseConnection(BaseConnection):
    """ connects to a couchbase server """

    def __init__(self, bucket, host=None, password=None):
        connstr = 'couchbase://{h}/{b}'.format(
            h=(host or 'localhost'),
            b=bucket )
        self._cb = Bucket(connstr, password=password)

    def get(self, key):
        result = self._cb.get(key, quiet=True)
        if result.success: return result.value

    def set(self, key, value):
        if key is None:
            key = uuid4().hex
        encoded_val = dumps(value)
        result = self._cb.upsert(key, value, persist_to=1)
        if result.success:
            return result.key, result.cas
        raise PersistenceError()

    def delete(self, key):
        return self._cb.remove(key, quiet=True)

    def query(self, design, name, **kw):
        return self._cb.query(design, name, **kw)

    def design_view_create(self, design, views, syncwait=5):
        """
        design => name of design document
        views => dict { 'viewname': {'map':...} }
        """
        views_d = {'views': views }
        return self._cb.design_create(
            design,
            views_d,
            use_devmode = False,
            syncwait=syncwait )

    def view_create(self, design, name, mapf, redf=None, syncwait=5):
        mapf = dedent(mapf.lstrip('\n'))
        redf = dedent(redf.lstrip('\n')) if redf else ''
        doc = { 'views': { name : { 'map': mapf, 'reduce': redf } } }
        self._cb.design_create(
            design,
            doc,
            use_devmode=False,
            syncwait=syncwait )

    def view_destroy(self, design):
        return self._cb.design_delete(design)

