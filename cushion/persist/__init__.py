
from json import dumps, loads
from textwrap import dedent
from uuid import uuid4

from .base import BaseConnection


ActiveConnection = None


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


