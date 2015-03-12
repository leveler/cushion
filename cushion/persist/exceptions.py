
class InvalidConnectionType(Exception):
    """ Wrong type of connection object provided """


class PersistenceNotConnected(Exception):
    """ No aceive persistence connection set. Maybe call set_connection(...) """
    pass


class PersistenceError(Exception):
    """ Error encountered during persistence of object """
    pass


