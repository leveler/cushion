
class InvalidConnectionType(Exception):
    """ Wrong type of connection object provided """


class PersistanceNotConnected(Exception):
    """ No active persistance connection set. Maybe call set_connection(...) """
    pass


class PersistanceError(Exception):
    """ Error encountered during persistance of object """
    pass


