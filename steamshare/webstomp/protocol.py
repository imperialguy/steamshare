from steamshare.webstomp import utils


class Protocol12(object):
    """
    Represents version 1.2 of the protocol (see https://stomp.github.io/stomp-specification-1.2.html).

    Most users should not instantiate the protocol directly. See :py:mod:`steamshare.stomp.connect` for connection classes.

    :param transport:
    :param (int,int) heartbeats:
    :param bool auto_content_length: Whether to calculate and send the content-length header automatically if it has not been set
    :param float heart_beat_receive_scale: how long to wait for a heartbeat before timing out, as a scale factor of receive time
    """
    def __init__(self,
                wsapp,
                logger,
                heartbeats=(0, 0),
                auto_content_length=True,
                heart_beat_receive_scale=1.5
                ):
        self.wsapp = wsapp
        self.logger = logger
        self.heartbeats = heartbeats
        self.heart_beat_receive_scale = heart_beat_receive_scale
        self.auto_content_length = auto_content_length
        self.version = "1.2"

    def on_message(self, message):
        bmsg = bytes(message, 'utf8')
        frame = utils.parse_frame(bmsg)
        if frame:
            self.logger.debug("frame command: {}".format(frame.cmd))
            self.logger.debug("frame headers: {}".format(frame.headers))
            self.logger.debug("frame body: {}".format(utils.decode(frame.body)))
        # self.logger.debug("\nmessage received\n{}".format(message))

    def _escape_headers(self, headers):
        """
        :param dict(str,str) headers:
        """
        for key, val in headers.items():
            try:
                val = val.replace("\\", "\\\\").replace("\n", "\\n").replace(":", "\\c").replace("\r", "\\r")
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
        if receipt:
            headers[utils.HDR_RECEIPT] = receipt
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

    def connect(self, username, passcode, vhost, wait=False, headers=None, **keyword_headers):
        """
        Send a STOMP CONNECT frame. Differs from 1.0 and 1.1 versions in that the HOST header is enforced.

        :param str username: optionally specify the login user
        :param str passcode: optionally specify the user password
        :param bool wait: wait for the connection to complete before returning
        :param dict headers: a map of any additional headers to send with the subscription
        :param keyword_headers: any additional headers to send with the subscription
        """
        self.wsapp.on_message = self.on_message
        cmd = utils.CMD_STOMP
        headers = utils.merge_headers([headers, keyword_headers])
        headers[utils.HDR_ACCEPT_VERSION] = self.version
        headers[utils.HDR_HOST] = vhost

        if username is not None:
            headers[utils.HDR_LOGIN] = username

        if passcode is not None:
            headers[utils.HDR_PASSCODE] = passcode

        self.send_frame(cmd, headers)

        # if wait:
        #     self.transport.wait_for_connection()
        #     if self.transport.connection_error:
        #         raise ConnectFailedException()

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
        # self.transport.transmit(frame)
        self.transmit(frame)

    def transmit(self, frame):
        lines = utils.convert_frame(frame)
        packed_frame = utils.pack(lines)

        self.wsapp.send(packed_frame)

    def abort(self, transaction, headers=None, **keyword_headers):
        """
        Abort a transaction.

        :param str transaction: the identifier of the transaction
        :param dict headers: a map of any additional headers the broker requires
        :param keyword_headers: any additional headers the broker requires
        """
        assert transaction is not None, "'transaction' is required"
        headers = utils.merge_headers([headers, keyword_headers])
        headers[utils.HDR_TRANSACTION] = transaction
        self.send_frame(utils.CMD_ABORT, headers)

    def begin(self, transaction=None, headers=None, **keyword_headers):
        """
        Begin a transaction.

        :param str transaction: the identifier for the transaction (optional - if not specified
            a unique transaction id will be generated)
        :param dict headers: a map of any additional headers the broker requires
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
        :param dict headers: a map of any additional headers the broker requires
        :param keyword_headers: any additional headers the broker requires
        """
        assert transaction is not None, "'transaction' is required"
        headers = utils.merge_headers([headers, keyword_headers])
        headers[utils.HDR_TRANSACTION] = transaction
        self.send_frame(utils.CMD_COMMIT, headers)

    def disconnect(self, receipt=None, headers=None, **keyword_headers):
        """
        Disconnect from the server.

        :param str receipt: the receipt to use (once the server acknowledges that receipt, we're
            officially disconnected; optional - if not specified a unique receipt id will
            be generated)
        :param dict headers: a map of any additional headers the broker requires
        :param keyword_headers: any additional headers the broker requires
        """
        # if not self.transport.is_connected():
        #     logging.debug("Not sending disconnect, already disconnected")
        #     return
        headers = utils.merge_headers([headers, keyword_headers])
        rec = receipt or utils.get_uuid()
        headers[utils.HDR_RECEIPT] = rec
        self.set_receipt(rec, utils.CMD_DISCONNECT)
        self.send_frame(utils.CMD_DISCONNECT, headers)

    def send(self, destination, body, content_type=None, headers=None, **keyword_headers):
        """
        Send a message to a destination in the messaging system (as per https://stomp.github.io/stomp-specification-1.2.html#SEND)

        :param str destination: the destination (such as a message queue - for example '/queue/test' - or a message topic)
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

    def subscribe(self, destination, id, ack="auto", headers=None, **keyword_headers):
        """
        Subscribe to a destination

        :param str destination: the topic or queue to subscribe to
        :param str id: the identifier to uniquely identify the subscription
        :param str ack: either auto, client or client-individual (see https://stomp.github.io/stomp-specification-1.2.html#SUBSCRIBE for more info)
        :param dict headers: a map of any additional headers to send with the subscription
        :param keyword_headers: any additional headers to send with the subscription
        """
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
        :param keyword_headers: any additional headers to send with the subscription
        """
        assert id is not None, "'id' is required"
        headers = utils.merge_headers([headers, keyword_headers])
        headers[utils.HDR_ID] = id
        self.send_frame(utils.CMD_UNSUBSCRIBE, headers)
