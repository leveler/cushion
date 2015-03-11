
import operator
from json import dumps, loads
from textwrap import dedent
from uuid import uuid4

import execjs
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


mapwrap = '''
function map_wrapper(doc, meta, outq) {
    function emit(key, val) {
        emit.outq.push([key, val, meta._id])
    }
    emit.outq = outq
    var mapf = %MAPF%
    mapf(doc, meta)
    return emit.outq
}
'''


class MemDoc(object):
    def __init__(self, docid, value):
        self.key = docid
        self.value = value


class MemResult(object):
    def __init__(self, key, docid, value, doc=None):
        self.key = key
        self.docid = docid
        self.value = value
        self.doc = doc


class MemResultSet(object):
    def __init__(self, results, include_docs=False):
        self._results = results
        self.include_docs = include_docs

    def __iter__(self):
        for r in self._results:
            yield r


class MemConnection(BaseConnection):

    def __init__(self):
        self.designs = {}
        self.data = {}

    def get(self, key):
        return self.data.get(key, None)

    def set(self, key, value):
        if key is None:
            key = uuid4().hex
        self.data[key] = value
        return key, uuid4().hex # fake cas

    def delete(self, key):
        del self.data[key]

    def query(self, design, name, **kw):
        # **note** NO REDUCE YET
        view_key = '/'.join((design, name))
        if view_key not in self.designs:
            raise Exception('view not found')
        view = self.designs[view_key]
        mapf_ctx = view.get('mapf')
        redf_ctx = view.get('redf')
        include_docs = False
        if 'include_docs' in kw:
            if kw['include_docs'] and kw['include_docs'] is not 'false':
                include_docs = True
        descending = bool(kw.get('descending', False))
        if descending:
            cmpop = operator.ge
        else:
            cmpop = operator.le
        outq = []
        # each entry looks like  [key, val, _id]
        for k,d in self.data.iteritems():
            meta = {'_id': k}
            outq = mapf_ctx.call('map_wrapper', d, meta, outq)
        if 'key' in kw:
            outq = filter(lambda x: cmp(x[0], kw['key'])==0, outq)
        if 'startkey' in kw:
            outq = filter(lambda x: cmpop(cmp(kw['startkey'], x[0]),0), outq)
        if 'endkey' in kw:
            outq = filter(lambda x: cmpop(cmp(x[0], kw['endkey']),0), outq)
        outq.sort(key=operator.itemgetter(0), reverse=descending)
        results = []
        for k in outq:
            r_ = MemResult(key=k[0], docid=k[2], value=k[1])
            if include_docs:
                r_.doc = MemDoc(k[2], self.data[k[2]])
            results.append(r_)
        if 'limit' in kw:
            results = results[:kw['limit']]
        return MemResultSet(results, include_docs)

    def design_view_create(self, design, views, syncwait=5):
        for v,d in views.iteritems():
            mapf = execjs.compile(mapwrap.replace('%MAPF%', d['map'].strip()))
            view = dict(mapf=mapf)
            self.designs["/".join((design, v))] = view

    def view_create(self, design, name, mapf, redf=None, syncwait=5):
        doc = { 'views': { name : { 'map': mapf, 'reduce': redf } } }
        self.design_view_create(design, doc)

    def view_destroy(self, design):
        prefix = design + "/"
        for k in self.designs.keys():
            if k.startswith(prefix):
                del self.designs[k]


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


def get_connection():
    global ActiveConnection
    return ActiveConnection


