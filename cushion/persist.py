
from json import dumps, loads
from textwrap import dedent
from uuid import uuid4

from couchbase import Couchbase

ActiveConnection = None


class InvalidConnectionType(Exception):
    """ Wrong type of connection object provided """


class PersistanceNotConnected(Exception):
    """ No active persistance connection set. Maybe call set_connection(...) """
    pass

class PersistanceError(Exception):
    """ Error encountered during persistance of object """
    pass


class BaseConnection(object):
    pass


class CouchbaseConnection(BaseConnection):

    def __init__(self, bucket, host=None, password=None, writeout=None):
        """
        writeout => if provided, a file handle to save out the raw commands
            passed to python couchbase. useful for debugging
        """
        self._cb = Couchbase.connect(bucket, host=host, password=password)
        self._wout = writeout

    def get(self, key):
        if self._wout:
            self._wout.write("db.get({})\n".format(dumps(key)))
        result = self._cb.get(key, quiet=True)
        if result.success: return result.value

    def set(self, key, value):
        if key is None:
            key = uuid4().hex
        encoded_val = dumps(value)
        if self._wout:
            self._wout.write("db.set({}, {})\n".format(
                dumps(key), value ))
        result = self._cb.set(key, value)
        if result.success:
            return result.key, result.cas
        raise PersistanceError()

    def delete(self, key):
        if self._wout:
            self._wout.write("db.delete({})\n".format(dumps(key)))
        return self._cb.delete(key)

    def query(self, design, name, **kw): 
        if self._wout:
            params = [ "{}='{}'".format(k,v) for k,v in kw.iteritems() ]
            self._wout.write("db.query({}, {}, {})\n".format(
                dumps(design), dumps(name), ", ".join(params) ))
        return self._cb.query(design, name, **kw)

    def design_view_create(self, design, views, syncwait=5):
        """
        design => name of design document
        views => dict { 'viewname': {'map':...} }
        """
        views_d = {'views': views }
        if self._wout:
            self._wout.write((
            "db.design_create({}, {}, use_devmode=False, syncwait={})\n"
            ).format(
                dumps(design), dumps(views_d), syncwait ))
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
        if self._wout:
            self._wout.write("db.design_delete({})\n".format(design))
        return self._cb.design_delete(design)


class Persist(object):
    """ proxy object for db related calls """

    def __init__(self, *a, **kw):
        if ActiveConnection is None:
            raise InvalidConnectionType()

    def get(self, docid):
        return ActiveConnection.get(docid)

    def set(self, docid, value):
        return ActiveConnection.set(docid, value)

    def delete(self, docid):
        return ActiveConnection.delete(docid)

    def query(self, *a, **kw):
        return ActiveConnection.query(*a, **kw)

    def view_create(self, *a, **kw):
        return ActiveConnection.view_create(*a, **kw)

    def view_destroy(self, *a, **kw):
        return ActiveConnection.view_destroy(*a, **kw)

    def design_view_create(self, *a, **kw):
        return ActiveConnection.design_view_create(*a, **kw)


def set_connection(conn):
    global ActiveConnection
    if not isinstance(conn, BaseConnection):
        raise InvalidConnectionType()
    ActiveConnection = conn


