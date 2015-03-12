
import operator
from uuid import uuid4

import execjs

from .base import BaseConnection


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


