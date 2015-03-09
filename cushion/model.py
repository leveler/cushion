
from .field import Field
from .persist import Persist
from .view import View



class DocTypeMismatch(Exception):
    """ Raised when document tries to instantiate from other doc type """


class DocTypeNotFound(Exception):
    """ Raised when document tries to instantiate without a doc type """


class NewModelClass(type):
    """ Metaclass for inheriting field lists """

    def __new__(cls, name, bases, attributes):
        # Emptying fields by default
        attributes["__fields"] = {}
        new_model = super(NewModelClass, cls).__new__(
            cls, name, bases, attributes)
        # pre-populate fields
        new_model._update_fields()
        return new_model

    def __setattr__(cls, name, value):
        """ Catching new field additions to classes """
        super(NewModelClass, cls).__setattr__(name, value)
        if isinstance(value, Field):
            # Update the fields, because they have changed
            cls._update_fields()



class Model(object):

    __metaclass__ = NewModelClass

    __fields = None
    __id = None

    @property
    def id(self):
        return self.__id

    @property
    def type(self):
        return self.__class__.__name__.lower()

    def __init__(self, *a, **kw):
        super(Model, self).__init__()
        self.__class__._update_fields()
        self._data = {}
        if 'type' in kw:
            # cleanup 'type' inbound
            if kw['type'] != self.type: raise DocTypeMismatch()
            del kw['type']
        if '_id' in kw:
            # existing model
            self.__id = kw['_id']
            del kw['_id']
        for k,v in kw.iteritems():
            setattr(self, k, v)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.id == other.id

    @classmethod
    def _update_fields(cls):
        cls.__fields = {}
        for attr_key in dir(cls):
            attr = getattr(cls, attr_key)
            if not isinstance(attr, Field):
                continue
            cls.__fields[attr.id] = attr_key

    @classmethod
    def load(cls, docid):
        doc = Persist().get(docid)
        if not doc: return None
        # only load fields with non None values
        return cls(
            _id=docid,
            **{k:v for k,v in doc.iteritems() if v is not None} )

    @property
    def _fields(self):
        """ Property wrapper for class fields """
        return self.__class__.__fields

    def save(self):
        data = {}
        for name in self.__fields.values():
            attr = getattr(self.__class__, name)
            data[name] = attr.to_d(self)
        data['type'] = self.type
        key, cas = Persist().set(self.__id, data)
        if not self.__id:
            self.__id = key
        self.__cas = cas
        return self

    def delete(self):
        return Persist().delete(self.__id)

    @classmethod
    def viewlist(cls):
        views = []
        for attr_key in dir(cls):
            attr = getattr(cls, attr_key)
            if isinstance(attr, View):
                views.append(attr)
        return views




