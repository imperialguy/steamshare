from steamshare.webstomp.listener import WaitingListener
from steamshare.utils.core import ObjectDict
from steamshare.webstomp import exceptions
from steamshare.webstomp import utils
import threading
import logging
import time
import queue

# result_available = threading.Event()

class ProtocolClient(object):
    """
    Represents version 1.2 of the protocol
    (see https://stomp.github.io/stomp-specification-1.2.html).

    """
    def __init__(self,
                logger,
                # wsapp,
                heartbeats=(0, 0),
                auto_content_length=True,
                heart_beat_receive_scale=1.5,
                headers=None,
                ping_interval=1,
                auto_decode=True,
                encoding="utf-8",
                is_eol_fc=utils.is_eol_default
                ):
        # self.wsapp = wsapp
        # self.wsapp.on_message = self.on_message
        self.logger = logger
        self.ping_interval = ping_interval
        self.heartbeats = heartbeats
        self.heart_beat_receive_scale = heart_beat_receive_scale
        self.auto_content_length = auto_content_length
        self.version = "1.2"
        self.connected = False
        self.connection_error = False
        self.disconnection_error = False
        self.listeners = {}
        self.__disconnect_receipt = None
        self.__ack_receipt = None
        self.__nack_receipt = None
        # self.__send_wait_condition = threading.Condition()
        # self.__connect_wait_condition = threading.Condition()
        # self.__diconnect_wait_condition = threading.Condition()
        # self.__ack_wait_condition = threading.Condition()
        # self.__nack_wait_condition = threading.Condition()
        # self.__listeners_change_condition = threading.Condition()
        self.__auto_decode = auto_decode
        self.__encoding = encoding
        self.__is_eol = is_eol_fc

    def set_listener(self, name, listener):
        """
        Set a named listener to use with this connection.
        See :py:class:`steamshare.stomp.listener.ConnectionListener`

        :param str name: the name of the listener
        :param ConnectionListener listener: the listener object
        """
        assert listener is not None
        with self.__listeners_change_condition:
            self.listeners[name] = listener

    def remove_listener(self, name):
        """
        Remove a listener according to the specified name

        :param str name: the name of the listener to remove
        """
        with self.__listeners_change_condition:
            del self.listeners[name]

    def get_listener(self, name):
        """
        Return the named listener

        :param str name: the listener to return

        :rtype: ConnectionListener
        """
        return self.listeners.get(name)

    def is_connected(self):
        return self.connected

    def set_connected(self, connected):
        """
        :param bool connected:
        """
        self.connected = connected
        # with self.__connect_wait_condition:
            # self.connected = connected
            # if connected:
            #     self.__connect_wait_condition.notifyAll()

        # if not connected:
        #     with self.__disconnect_wait_condition:
        #         self.__disconnect_receipt = None
        #         self.__disconnect_wait_condition.notify()

    def set_receipt(self, receipt_id, value):
        if value:
            self.__receipts[receipt_id] = value
        elif receipt_id in self.__receipts:
            del self.__receipts[receipt_id]

    def on_message(self, message):
        # self.print_lock.release()
        # self.print_lock.acquire(True, 1.0)
        self.logger.debug('Received message\n{}'.format(message))
        frame = utils.parse_frame(utils.encode(message))
        # frame = utils.parse_frame(bytes(message))
        if frame:
            self.process_frame(frame, message)
        else:
            self.logger.debug("Couldn't build a frame from the following "
                "message: {}".format(message))

    def process_frame(self, frame, frame_str):
        """
        :param Frame f: Frame object
        :param bytes frame_str: raw frame content
        """
        # for t in threading.enumerate():
        #     if t != threading.current_thread() and t != threading.main_thread():
        #         t.join(1)
        # for t in threading.enumerate():
        #     # if t.getName() == 'Thread-1':
        #     #     t.join(0.0)
        #     self.logger.debug('thread %s', t.getName())
        frame_type = frame.cmd.lower()

        if not frame_type in ["connected", "message", "receipt",
                "error", "heartbeat"]:
            self.logger.warning("Unknown response frame type: '%s' "
                "(frame length was %d)", frame_type, length(frame_str))

            return

        self.logger.debug('Received frame: %r, headers=%r, body=%r',
                    frame.cmd, frame.headers, frame.body)

        # if frame_type == "receipt":
        #     receipt = frame.headers["receipt-id"]
        #     receipt_value = self.__receipts.get(receipt)

            # with self.__send_wait_condition:
            #     self.set_receipt(receipt, None)
            #     self.__send_wait_condition.notify()

            # if receipt_value == utils.CMD_DISCONNECT and \
            #     receipt == self.__disconnect_receipt:
            #         self.set_connected(False)
            #
            # if receipt_value == utils.CMD_ACK and \
            #     receipt == self.__ack_receipt:
            #         with self.__ack_wait_condition():
            #             self.__ack_receipt = None
            #
            # if receipt_value == utils.CMD_NACK and \
            #     receipt == self.__nack_receipt:
            #         self.__nack_receipt = None

        # elif frame_type == "connected":
        if frame_type == "connected":
            self.set_connected(True)

        # elif frame_type == "disconnected":
        #     self.set_connected(False)

        elif frame_type == "error" and not self.connected:
            self.logger.debug('Error connecting')
            # with self.__connect_wait_condition:
            self.connection_error = True
                # self.__connect_wait_condition.notifyAll()
                # self.__connect_wait_condition.notify()
        # self.print_lock.release()

        # elif frame_type == "error" and self.connected:
        #     with self.__disconnect_wait_condition:
        #         self.disconnection_error = True
        #         self.__disconnect_wait_condition.notify()

        self.notify_listeners(frame_type, frame)

    def notify_listeners(self, frame_type, frame=None):
        """
        Utility function for notifying listeners of incoming/outgoing messages

        :param str frame_type: the type of message
        :param dict headers: the map of headers associated with the message
        :param body: the content of the message

        """
        self.logger.debug('notifying listeners')
        # with self.__listeners_change_condition:
        listeners = sorted(self.listeners.items())

        self.logger.debug('listeners:{}'.format(listeners))
        for (listener_name, listener) in listeners:
            self.logger.debug('notifying {}'.format(listener_name))
            notify_func = getattr(listener, "on_%s" % frame_type, None)

            if notify_func:
                notify_func(frame)
            else:
                self.logger.debug("listener %s has no method on_%s", listener,
                frame_type)

    def _escape_headers(self, headers):
        """
        :param dict(str,str) headers:
        """
        for key, val in headers.items():
            try:
                val = val.replace("\\", "\\\\").replace("\n", "\\n").replace(
                        ":", "\\c").replace("\r", "\\r")
            except:
                pass
            headers[key] = val

    def ack(self, id, transaction=None, receipt=None):
        """
        Acknowledge 'consumption' of a message by id.

        :param str id: identifier of the message
        :param str transaction: include the acknowledgement in the specified transaction
        :param str receipt: the receipt id
        """
        assert id is not None, "'id' is required"
        headers = {utils.HDR_ID: id}
        if transaction:
            headers[utils.HDR_TRANSACTION] = transaction
        rec = receipt or utils.get_uuid()
        headers[utils.HDR_RECEIPT] = rec
        self.set_receipt(rec, utils.CMD_ACK)
        self.send_frame(utils.CMD_ACK, headers)

    def nack(self, id, transaction=None, receipt=None, **keyword_headers):
        """
        Let the server know that a message was not consumed.

        :param str id: the unique id of the message to nack
        :param str transaction: include this nack in a named transaction
        :param str receipt: the receipt id
        :param keyword_headers: any additional headers to send with the nack command
        """
        assert id is not None, "'id' is required"
        headers = {utils.HDR_ID: id}
        headers = utils.merge_headers([headers, keyword_headers])
        if transaction:
            headers[utils.HDR_TRANSACTION] = transaction
        if receipt:
            headers[utils.HDR_RECEIPT] = receipt
        self.send_frame(utils.CMD_NACK, headers)

    def connect(self, username, passcode, vhost, headers=None,
            **keyword_headers):
        """
        Send a STOMP CONNECT frame. Differs from 1.0 and 1.1 versions in that
        the HOST header is enforced.

        :param str username: optionally specify the login user
        :param str passcode: optionally specify the user password
        :param bool wait: wait for the connection to complete before returning
        :param dict headers: additional headers to send with the subscription
        :param keyword_headers: additional headers to send with subscription
        """
        cmd = utils.CMD_STOMP
        headers = utils.merge_headers([headers, keyword_headers])
        headers[utils.HDR_ACCEPT_VERSION] = self.version
        headers[utils.HDR_HOST] = vhost

        if username is not None:
            headers[utils.HDR_LOGIN] = username

        if passcode is not None:
            headers[utils.HDR_PASSCODE] = passcode

        return self.send_frame(cmd, headers)

        # queue_obj = queue.Queue()
        # event = threading.Event()
        # self.print_lock = threading.Lock()
        # self.print_lock.acquire(False)

        # self.logger.debug('before connection_error: {}'.format(self.connection_error))
        #
        # thread_obj = threading.Thread(name='connect_wait_thread',
        #                             target=self.wait_for_connection_thread,
        #                             # args=(queue_obj, t_stop)
        #                             )
        # # thread_obj_1 = threading.Thread(name='connect_wait_thread_1',
        # #                             target=self.waiter_thread,
        # #                             args=(t_stop,))
        # # thread_obj.setDaemon(True)
        # thread_obj.start()
        # # result_available.wait()
        # thread_obj.join(2)
        # self.logger.debug('after connection_error: {}'.format(self.connection_error))
        # thread_obj_1.start()
        # for t in threading.enumerate():
        #     if t != threading.current_thread() and t.getName() != 'Thread-1' and t != threading.main_thread():
        #         t.join(1)
        # time.sleep(2)
        # t_stop.wait()
        # thread_obj.join()
        # self.wait_for_connection_thread(queue_obj)
        # while thread_obj.isAlive():
        #     self.logger.debug('Waiting')

        # self.ws_thread.start()

        # while True:
        #     try:
        #         queue_obj.get(block=False)
        #     except queue.Empty:
        #         pass
        #     else:
        #         raise exceptions.ConnectFailedException()
        #
        #     # thread_obj.join(0.1)
        #     if thread_obj.isAlive():
        #         continue
        #     else:
        #         break

        # if not self.is_connected() or not self.connection_error:
        #     self.logger.debug('Connection failed')

    # def wait_for_connection(self, thread_obj, queue_obj, timeout=None):
    #     thread_obj.__connect_wait_thread.start()

    def waiter_thread(self, event):
        self.logger.debug("Waiting for event")
        if event.wait(5):
            self.logger.debug("event set.")
        else:
            self.logger.debug("Timed out.")

    def wait_for_connection_thread(self, timeout=None):
        """
        Wait until we've established a connection with the virtual host.

        :param float timeout: how long to wait, in seconds
        """
        # self.event = threading.Event()
        # self.print_lock.release()
        # self.print_lock.acquire(False)
        with self.__connect_wait_condition:
            while not self.is_connected() and not self.connection_error:
                self.logger.debug('Connection wait condition - waiting')
                self.__connect_wait_condition.wait()
                self.logger.debug('Connection wait condition - notified')

        # result_available.set()

            # self.logger.debug('releasing lock')
                # for t in threading.enumerate():
                #     if t.getName() == 'Thread-1':
                #         t.start()

        # if not self.is_connected() and not self.connection_error
        # event.set()
        # queue_obj.put('raise exception')
        # threading.main_thread().join()

    def disconnect(self, receipt=None, headers=None, **keyword_headers):
        """
        Disconnect from the server.

        :param str receipt: the receipt to use
        (once server acknowledges that receipt, we're officially disconnected;
        optional - if not specified a unique receipt id will be generated)
        :param dict headers: map of additional headers the broker requires
        :param keyword_headers: any additional headers the broker requires
        """
        # if not self.transport.is_connected():
        #     logging.debug("Not sending disconnect, already disconnected")
        #     return
        self.logger.debug('Disconnect attempt')
        headers = utils.merge_headers([headers, keyword_headers])
        rec = receipt or utils.get_uuid()
        headers[utils.HDR_RECEIPT] = rec
        # self.set_receipt(rec, utils.CMD_DISCONNECT)

        # self.logger.debug('Disconnect attempt using receipt id: {}'.format(rec))
        # self.disconnect_receipt_listener = WaitingListener(self.logger, rec)
        # self.set_listener("disconnect-receipt-listener",
        #     self.disconnect_receipt_listener)
        self.send_frame(utils.CMD_DISCONNECT, headers)
        # self.disconnect_receipt_listener.wait_on_receipt()
        #
        # if not self.disconnect_receipt_listener.received:
        #     raise exceptions.DisconnectFailedException()
        # self.set_connected(False)

    # def wait_for_disconnection(self, timeout=None):
    #     """
    #     Wait until we've disconnected with the virtual host.
    #
    #     :param float timeout: how long to wait, in seconds
    #     """
    #     if timeout is not None:
    #         wait_time = timeout / 10.0
    #     else:
    #         wait_time = None
    #
    #     with self.__diconnect_wait_condition:
    #         while self.is_connected() and self.__disconnect_receipt \
    #                 is not None:
    #             self.__disconnect_wait_condition.wait(wait_time)
    #
    #     if self.is_connected():
    #         raise exception.DisconnectFailedException()

    def send_frame(self, cmd, headers=None, body=''):
        """
        Encode and send a stomp frame
        through the underlying transport:

        :param str cmd: the protocol command
        :param dict headers: a map of headers to include in the frame
        :param body: the content of the message
        """
        if cmd != utils.CMD_CONNECT:
            if headers is None:
                headers = {}
            self._escape_headers(headers)
        frame = utils.Frame(cmd, headers, body)
        if frame.cmd == utils.CMD_CONNECT or frame.cmd == utils.CMD_STOMP:
            if self.heartbeats != (0, 0):
                frame.headers[utils.HDR_HEARTBEAT] = "%s,%s" % self.heartbeats

        return self.transmit(frame)

    def transmit(self, frame):
        # with self.__listeners_change_condition:
        listeners = sorted(self.listeners.items())

        for (_, listener) in listeners:
            try:
                listener.on_send(frame)
            except AttributeError:
                continue

        # if frame.cmd == utils.CMD_DISCONNECT and utils.HDR_RECEIPT in \
        #         frame.headers:
        #     self.__disconnect_receipt = frame.headers[utils.HDR_RECEIPT]
        # if frame.cmd == utils.CMD_ACK and utils.HDR_RECEIPT in \
        #         frame.headers:
        #     self.__ack_receipt = frame.headers[utils.HDR_RECEIPT]
        # if frame.cmd == utils.CMD_NACK and utils.HDR_RECEIPT in \
        #         frame.headers:
        #     self.__nack_receipt = frame.headers[utils.HDR_RECEIPT]

        lines = utils.convert_frame(frame)
        packed_frame = utils.pack(lines)

        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("Building frame: %s", utils.clean_lines(lines))
            # self.logger.debug("Sending frame: %s %s", packed_frame, type(
            #     packed_frame))
        else:
            self.logger.info("Building frame: %r", frame.cmd or "heartbeat")

        return packed_frame
        # self.wsapp.send(packed_frame)

    def begin(self, transaction=None, headers=None, **keyword_headers):
        """
        Begin a transaction.

        :param str transaction: the identifier for the transaction
        (optional - if not specified a unique transaction id will be generated)
        :param dict headers: any additional headers the broker requires
        :param keyword_headers: any additional headers the broker requires

        :return: the transaction id
        :rtype: str
        """
        headers = utils.merge_headers([headers, keyword_headers])
        if not transaction:
            transaction = utils.get_uuid()
        headers[utils.HDR_TRANSACTION] = transaction
        self.send_frame(utils.CMD_BEGIN, headers)
        return transaction

    def commit(self, transaction=None, headers=None, **keyword_headers):
        """
        Commit a transaction.

        :param str transaction: the identifier for the transaction
        :param dict headers: any additional headers the broker requires
        :param keyword_headers: any additional headers the broker requires
        """
        assert transaction is not None, "'transaction' is required"
        headers = utils.merge_headers([headers, keyword_headers])
        headers[utils.HDR_TRANSACTION] = transaction
        self.send_frame(utils.CMD_COMMIT, headers)

    def abort(self, transaction, headers=None, **keyword_headers):
        """
        Abort a transaction.

        :param str transaction: the identifier of the transaction
        :param dict headers: any additional headers the broker requires
        :param keyword_headers: any additional headers the broker requires
        """
        assert transaction is not None, "'transaction' is required"
        headers = utils.merge_headers([headers, keyword_headers])
        headers[utils.HDR_TRANSACTION] = transaction
        self.send_frame(utils.CMD_ABORT, headers)

    def send(self, destination, body, content_type=None, headers=None,
        **keyword_headers):
        """
        Send a message to a destination in the messaging system
        (as per https://stomp.github.io/stomp-specification-1.2.html#SEND)

        :param str destination: the destination
        (such as a message queue - for e.g. '/queue/test' - or a message topic)
        :param body: the content of the message
        :param str content_type: the MIME type of message
        :param dict headers: additional headers to send in the message frame
        :param keyword_headers: any additional headers the broker requires
        """
        assert destination is not None, "'destination' is required"
        assert body is not None, "'body' is required"
        headers = utils.merge_headers([headers, keyword_headers])
        headers[utils.HDR_DESTINATION] = destination
        if content_type:
            headers[utils.HDR_CONTENT_TYPE] = content_type
        if self.auto_content_length and body and utils.HDR_CONTENT_LENGTH not \
                in headers:
            headers[utils.HDR_CONTENT_LENGTH] = len(body)
        self.send_frame(utils.CMD_SEND, headers, body)

    def subscribe(self, destination, id, ack="client", headers=None,
                **keyword_headers):
        """
        Subscribe to a destination

        :param str destination: the topic or queue to subscribe to
        :param str id: the identifier to uniquely identify the subscription
        :param str ack: either auto, client or client-individual
        (see https://stomp.github.io/stomp-specification-1.2.html#SUBSCRIBE)
        :param dict headers: additional headers to send with subscription
        :param keyword_headers: additional headers to send with subscription
        """
        self.logger.debug('Subscribing')
        assert destination is not None, "'destination' is required"
        assert id is not None, "'id' is required"
        headers = utils.merge_headers([headers, keyword_headers])
        headers[utils.HDR_DESTINATION] = destination
        headers[utils.HDR_ID] = id
        headers[utils.HDR_ACK] = ack
        self.send_frame(utils.CMD_SUBSCRIBE, headers)

    def unsubscribe(self, id, headers=None, **keyword_headers):
        """
        Unsubscribe from a destination by its unique identifier

        :param str id: the unique identifier to unsubscribe from
        :param dict headers: additional headers to send with the unsubscribe
        :param keyword_headers: additional headers to send with unsubscribe
        """
        assert id is not None, "'id' is required"
        headers = utils.merge_headers([headers, keyword_headers])
        headers[utils.HDR_ID] = id
        self.send_frame(utils.CMD_UNSUBSCRIBE, headers)


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
            self.logger.debug('Encountered a NoRollbackError. '
                                'Ignoring rollback.')

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
