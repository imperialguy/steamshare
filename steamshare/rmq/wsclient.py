from steamshare.stomp import WebSocketConnection
from steamshare.utils.core import ObjectDict

class RMQWSClient(object):
    def __new__(cls, logger, url, vhost, user, password, heartbeats=(0, 0),
                encoding="utf-8"):
        ws_connection = WebSocketConnection(logger, url, vhost, heartbeats)
        ws_connection.connect(user, password)

        return ws_connection

# class RMQWSMQTTClient(object):
#     def __new__(cls, logger, url, vhost, user, password, heartbeats=(0, 0),
#                 encoding="utf-8"):
#         ws_connection = WebSocketConnection(logger, url, vhost, heartbeats)
#         ws_connection.connect(user, password)
#
#         return ws_connection


class RMQWSTransaction(object):
    def __init__(self, logger, connection):
        """ Constructor to setup the connection object

        """
        self.logger = logger
        self.connection = connection

    def __enter__(self):
        """ Starts transaction by returning a connection object

        """
        self.logger.debug('Started Stomp Transaction')
        txid = self.connection.begin()

        return ObjectDict(connection=self.connection, id=txid)

    def __exit__(self, exception_type, exception_value, exception_traceback):
        if exception_type == NoRollbackError:
            self.logger.debug('Encountered a NoRollbackError. Ignoring rollback.')

        if exception_type and exception_type != NoRollbackError:
            self.connection.abort()
            self.logger.exception(
                'Rolling back stomp transaction because the following '
                'exception occurred\n')
        else:
            self.connection.commit()
            self.logger.debug('Commit successful!')

        # self.connection.disconnect()

        self.logger.debug('Ended Stomp Transaction')
