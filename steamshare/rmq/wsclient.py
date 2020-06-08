from steamshare.webstomp.pclient import ProtocolClient
import websocket

# websocket.enableTrace(True)
class RMQWSClient:
    @classmethod
    def connect(cls, logger, url, subscriber, ping_interval=1):
        cls.logger = logger
        cls.url = url
        cls.subscriber = subscriber
        cls.ping_interval = ping_interval
        cls.wsapp = websocket.WebSocketApp(cls.url,
                                    # header=headers,
                                    on_open=cls.on_open,
                                    on_error=cls.on_error,
                                    on_close=cls.on_close
                                    )
        cls.wsapp.run_forever(ping_interval=cls.ping_interval)

    @staticmethod
    def on_error(wsapp, exception):
        RMQWSClient.process_error(exception)

    @classmethod
    def process_error(cls, exception):
        cls.logger.exception('Following exception occured in the websocket '
                        'connection to {}\n{}'.format(cls.url, str(exception)))

    @classmethod
    def process_closed(cls):
        cls.logger.debug('CLOSED websocket connection to {}'.format(cls.url))

    @staticmethod
    def on_close(wsapp):
        RMQWSClient.process_closed()

    @classmethod
    def process_open(cls, wsapp):
        wsclient = ProtocolClient(cls.logger, wsapp)
        cls.subscriber(wsclient)

    @staticmethod
    def on_open(wsapp):
        RMQWSClient.process_open(wsapp)


# class RMQWSTransaction(object):
#     def __init__(self, logger, connection):
#         """ Constructor to setup the connection object
#
#         """
#         self.logger = logger
#         self.connection = connection
#
#     def __enter__(self):
#         """ Starts transaction by returning a connection object
#
#         """
#         self.logger.debug('Started Stomp Transaction')
#         txid = self.connection.begin()
#
#         return ObjectDict(connection=self.connection, id=txid)
#
#     def __exit__(self, exception_type, exception_value, exception_traceback):
#         if exception_type == NoRollbackError:
#             self.logger.debug('Encountered a NoRollbackError. Ignoring rollback.')
#
#         if exception_type and exception_type != NoRollbackError:
#             self.connection.abort()
#             self.logger.exception(
#                 'Rolling back stomp transaction because the following '
#                 'exception occurred\n')
#         else:
#             self.connection.commit()
#             self.logger.debug('Commit successful!')
#
#         # self.connection.disconnect()
#
#         self.logger.debug('Ended Stomp Transaction')
