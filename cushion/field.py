
from base64 import b64decode, b64encode

from datetime import datetime
from json import loads
import iso8601


class Field(object):

    def __init__(self, loader=None, default=None):
        """
        initializes this field.

        loader - optional function that can do any initialization and
                 validation necessary to ensure the value is properly
                 loaded in the field
        default - optional scalar or callable to initialize the value
        """
        self._loader = loader
        self._default = default
        self.id = id(self)
        self._field_name = None

    def _get_fieldname(self, model_instance):
        if self._field_name:
            return self._field_name
        fields = getattr(model_instance, "_fields")
        return fields[self.id]

    def _get_value(self, instance):
        field_name = self._get_fieldname(instance)
        value = None
        if field_name in instance._data:
            return instance._data.get(field_name)
        default = self._default
        if default is not None:
            default_val = default() if callable(default) else default
            if self._loader:
                instance._data[field_name] = self._loader(default_val)
            else:
                instance._data[field_name] = default_val
        return instance._data.get(field_name)

    def to_d(self, instance):
        return self._get_value(instance)

    def __set__(self, instance, value):
        field_name = self._get_fieldname(instance)
        v_ = self._loader(value) if self._loader else value
        instance._data[field_name] = v_

    def __get__(self, instance, cls=None):
        if instance is None:
            # called by class, which gets the Field
            return self
        return self._get_value(instance)


class RefField(Field):

    def __set__(self, instance, value):
        # we will override this completely to enable late binding/loading of
        # ref'd objects
        field_name = self._get_fieldname(instance)
        if isinstance(value, basestring):
            # assume id
            instance._raw_data[field_name] = value
        elif isinstance(value, self._cls):
            # given a model
            instance._raw_data[field_name] = value.id
            instance._data[field_name] = value

    def __get__(self, instance, cls=None):
        if instance is None:
            # called at class, get the field
            return self
        return self._get_value(instance)

    def _get_value(self, instance):
        field_name = self._get_fieldname(instance)
        if field_name not in instance._data:
            if field_name in instance._raw_data:
                # not loaded, let's load it if we have id
                doc = self._doc_loader(instance._raw_data[field_name])
            else:
                if self._default:
                    default = self._default
                    doc = default() if callable(default) else default
                    doc = self._doc_loader(doc)
                else:
                    doc = None
            # now let's save this value for the next time we want it
            instance._data[field_name] = doc
        return instance._data[field_name]

    def _doc_loader(self, value):
        if not value:
            # don't try to load nothing
            return
        if isinstance(value, basestring):
            # assume it's an id and try to load the model
            return self._cls.load(value)
        if isinstance(value, self._cls):
            # we're one of those already, just return it
            return value

    def __init__(self, cls):
        self._cls = cls
        assert hasattr(cls, 'load') # ensure .load exists, Models have it
        # overload the loader
        super(RefField, self).__init__()

    def to_d(self, instance):
        field_name = self._get_fieldname(instance)
        if field_name in instance._raw_data and instance._raw_data[field_name]:
            # the id should be cached here, return that
            return instance._raw_data[field_name]
        val = self._get_value(instance)
        if not val: return ''
        if val and not val.id:
            # not saved yet, can't serialize?  save here.
            val.save()
        return val.id


class TextField(Field):

    def __init__(self, default=None):
        super(TextField, self).__init__(loader=unicode, default=default)

    def to_d(self, instance):
        return self._get_value(instance) or ''


class ByteField(Field):
    """
    Loads b64encoded data from the database.
    Must be assigned b64 data when created.
    """

    def _byte_loader(self, instr):
        return b64decode(instr)

    def __init__(self, **kw):
        super(ByteField, self).__init__(loader=self._byte_loader, **kw)

    def to_d(self, instance):
        return b64encode(self._get_value(instance) or '')


class BooleanField(Field):

    def __init__(self, **kw):
        super(BooleanField, self).__init__(loader=bool, **kw)


class FloatField(Field):

    def __init__(self, default=None):
        super(FloatField, self).__init__(
            loader=lambda v: float(v) if v is not None else None,
            default=default )


class IntegerField(Field):

    def __init__(self, default=0):
        super(IntegerField, self).__init__(
            loader=lambda v: int(v) if v is not None else None,
            default=default )


class ListField(Field):

    def _list_loader(self, ll):
        if ll is None:
            # no prob
            return []
        if isinstance(ll, list):
            # already good, go with that
            return ll
        if isinstance(ll, basestring):
            # try the json load
            l_ = loads(ll)
            assert isinstance(l_, list), "failed to load list from json"
            return l_

    def __init__(self, **kw):
        if 'default' not in kw:
            kw['default'] = list
        super(ListField, self).__init__(loader=self._list_loader, **kw)


class DictField(Field):

    def _dict_loader(self, dd):
        if dd is None:
            return {}
        if isinstance(dd, dict):
            return dd
        if isinstance(dd, basestring):
            d_ = loads(dd)
            assert isinstance(d_, dict), "failed to load dict from json"
            return d_

    def __init__(self, **kw):
        if 'default' not in kw:
            kw['default'] = dict
        super(DictField, self).__init__(loader=self._dict_loader, **kw)


class DateTimeField(Field):

    def _load_date(self, dt):
        if not dt:
            # sometimes, we get handed a None, don't fret with it
            return
        if isinstance(dt, datetime):
            return dt
        if isinstance(dt, basestring):
            try:
                if not self._naive:
                    return iso8601.parse_date(dt)
                else:
                    # If dt is without a time zone then this will return a naive dt
                    # else it will handle it like the above
                    return iso8601.parse_date(dt, default_timezone=None)
            except iso8601.ParseError:
                raise ValueError('invalid date format')
        raise ValueError('date must be iso8601 or datetime object')

    def __init__(self, **kw):
        self._naive = False
        if 'naive' in kw:
            self._naive = kw['naive']
            del kw['naive']
        super(DateTimeField, self).__init__(loader=self._load_date, **kw)

    def to_d(self, instance):
        val = self._get_value(instance)
        return val.isoformat() if val else ''


