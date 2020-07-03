from steamshare.webstomp.utils import (
    StompUtils,
    StompFrame
)


class StompProtocolManager(object):
    """
    Represents version 1.2 of the protocol
    (see https://stomp.github.io/stomp-specification-1.2.html).

    """
    def __init__(self,
                logger,
                user,
                password,
                vhost,
                heartbeats=(0, 0),
                auto_content_length=True,
                heart_beat_receive_scale=1.5,
                headers=None,
                ping_interval=1,
                auto_decode=True,
                encoding="utf-8",
                is_eol_fc=StompUtils.is_eol_default
                ):
        self.logger = logger
        self.user = user
        self.password = password
        self.vhost = vhost
        self.ping_interval = ping_interval
        self.heartbeats = heartbeats
        self.heart_beat_receive_scale = heart_beat_receive_scale
        self.auto_content_length = auto_content_length
        self.version = "1.2"
        self.__auto_decode = auto_decode
        self.__encoding = encoding
        self.__is_eol = is_eol_fc
        self.receipts = dict()

    def set_receipt(self, receipt_id):
        self.logger.debug('Setting event for receipt id: {}'.format(
            receipt_id))
        self.receipts[receipt_id] = trio.Event()

    def get_receipt(self, receipt_id):
        return self.receipts[receipt_id]

    def delete_receipt(self, receipt_id):
        try:
            del self.receipts[receipt_id]
        except KeyError:
            pass
        else:
            self.logger.debug('Deleted trio event for receipt : {}'.format(
                receipt_id))

    def mark_receipt_read(self, receipt_id):
        self.logger.debug('Marking receipt id: {} as READ'.format(receipt_id))
        self.receipts[receipt_id].set()

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

    def ack(self, id, receipt, transaction=None):
        """
        Acknowledge 'consumption' of a message by id.

        :param str id: identifier of the message
        :param str transaction: include the ack in the specified transaction
        :param str receipt: the receipt id
        """
        assert id is not None, "'id' is required"
        headers = {StompUtils.HDR_ID: id}
        if transaction:
            headers[StompUtils.HDR_TRANSACTION] = transaction
        headers[StompUtils.HDR_RECEIPT] = receipt
        return self.send_frame(StompUtils.CMD_ACK, headers)

    def nack(self, id, receipt, transaction=None, **keyword_headers):
        """
        Let the server know that a message was not consumed.

        :param str id: the unique id of the message to nack
        :param str transaction: include this nack in a named transaction
        :param str receipt: the receipt id
        :param keyword_headers: additional headers to send with nack command
        """
        assert id is not None, "'id' is required"
        headers = {StompUtils.HDR_ID: id}
        headers = StompUtils.merge_headers([headers, keyword_headers])
        headers[StompUtils.HDR_RECEIPT] = receipt
        if transaction:
            headers[StompUtils.HDR_TRANSACTION] = transaction
        return self.send_frame(StompUtils.CMD_NACK, headers)

    def connect(self, user=None, password=None, vhost=None, headers=None,
            **keyword_headers):
        """
        Send a STOMP CONNECT frame. Differs from 1.0 and 1.1 versions in that
        the HOST header is enforced.

        :param str user: optionally specify the login user
        :param str password: optionally specify the user password
        :param bool wait: wait for the connection to complete before returning
        :param dict headers: additional headers to send with the subscription
        :param keyword_headers: additional headers to send with subscription
        """
        cmd = StompUtils.CMD_STOMP
        headers = StompUtils.merge_headers([headers, keyword_headers])
        headers[StompUtils.HDR_ACCEPT_VERSION] = self.version
        headers[StompUtils.HDR_HOST] = vhost if vhost else self.vhost
        headers[StompUtils.HDR_LOGIN] = user if user else self.user
        headers[StompUtils.HDR_PASSCODE] = password if password else \
            self.password

        return self.send_frame(cmd, headers)

    def disconnect(self, receipt, headers=None, **keyword_headers):
        """
        Disconnect from the server.

        :param str receipt: the receipt to use
        (once server acknowledges that receipt, we're officially disconnected;
        optional - if not specified a unique receipt id will be generated)
        :param dict headers: map of additional headers the broker requires
        :param keyword_headers: any additional headers the broker requires
        """
        self.logger.debug('Disconnect attempt')
        headers = StompUtils.merge_headers([headers, keyword_headers])
        headers[StompUtils.HDR_RECEIPT] = receipt

        return self.send_frame(StompUtils.CMD_DISCONNECT, headers)

    def send_frame(self, cmd, headers=None, body=''):
        """
        Encode and send a stomp frame
        through the underlying transport:

        :param str cmd: the protocol command
        :param dict headers: a map of headers to include in the frame
        :param body: the content of the message
        """
        if cmd != StompUtils.CMD_CONNECT:
            if headers is None:
                headers = {}
            self._escape_headers(headers)
        frame = StompFrame(cmd, headers, body)
        if frame.cmd == StompUtils.CMD_CONNECT or frame.cmd == \
                StompUtils.CMD_STOMP:
            if self.heartbeats != (0, 0):
                frame.headers[StompUtils.HDR_HEARTBEAT] = '%s,%s' \
                    % self.heartbeats

        return self.transmit(frame)

    def transmit(self, frame):
        lines = StompUtils.convert_frame(frame)
        packed_frame = StompUtils.pack(lines)

        self.logger.debug("Building frame: %s", StompUtils.clean_lines(lines))

        return packed_frame

    def begin(self, transaction=None, headers=None, **keyword_headers):
        """
        Begin a transaction.

        :param str transaction: the identifier for the transaction
        (optional; if not specified a unique transaction id will be generated)
        :param dict headers: any additional headers the broker requires
        :param keyword_headers: any additional headers the broker requires

        :return: the transaction id
        :rtype: str
        """
        headers = StompUtils.merge_headers([headers, keyword_headers])
        if not transaction:
            transaction = StompUtils.get_uuid()
        headers[StompUtils.HDR_TRANSACTION] = transaction
        return transaction, self.send_frame(StompUtils.CMD_BEGIN, headers)

    def commit(self, transaction=None, headers=None, **keyword_headers):
        """
        Commit a transaction.

        :param str transaction: the identifier for the transaction
        :param dict headers: any additional headers the broker requires
        :param keyword_headers: any additional headers the broker requires
        """
        assert transaction is not None, "'transaction' is required"
        headers = StompUtils.merge_headers([headers, keyword_headers])
        headers[StompUtils.HDR_TRANSACTION] = transaction
        return self.send_frame(StompUtils.CMD_COMMIT, headers)

    def abort(self, transaction, headers=None, **keyword_headers):
        """
        Abort a transaction.

        :param str transaction: the identifier of the transaction
        :param dict headers: any additional headers the broker requires
        :param keyword_headers: any additional headers the broker requires
        """
        assert transaction is not None, "'transaction' is required"
        headers = StompUtils.merge_headers([headers, keyword_headers])
        headers[StompUtils.HDR_TRANSACTION] = transaction
        return self.send_frame(StompUtils.CMD_ABORT, headers)

    def send(self, destination, body, content_type=None, headers=None,
        **keyword_headers):
        """
        Send a message to a destination in the messaging system
        (as per https://stomp.github.io/stomp-specification-1.2.html#SEND)

        :param str destination: the destination
        (such as a message queue - e.g. '/queue/test' - or a message topic)
        :param body: the content of the message
        :param str content_type: the MIME type of message
        :param dict headers: additional headers to send in the message frame
        :param keyword_headers: any additional headers the broker requires
        """
        assert destination is not None, "'destination' is required"
        assert body is not None, "'body' is required"
        headers = StompUtils.merge_headers([headers, keyword_headers])
        headers[StompUtils.HDR_DESTINATION] = destination
        if content_type:
            headers[StompUtils.HDR_CONTENT_TYPE] = content_type
        if self.auto_content_length and body and \
                StompUtils.HDR_CONTENT_LENGTH not in headers:
            headers[StompUtils.HDR_CONTENT_LENGTH] = len(body)
        return self.send_frame(StompUtils.CMD_SEND, headers, body)

    def subscribe(self, destination, id=None, ack="client", headers=None,
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
        assert destination is not None, "'destination' is required"
        id = id if id else random.randint(1, 1000)
        self.logger.debug('Subscribing to {}'.format(destination))
        headers = StompUtils.merge_headers([headers, keyword_headers])
        headers[StompUtils.HDR_DESTINATION] = destination
        headers[StompUtils.HDR_ID] = id if id else random.randint(1, 1000)
        headers[StompUtils.HDR_ACK] = ack
        return id, self.send_frame(StompUtils.CMD_SUBSCRIBE, headers)

    def unsubscribe(self, id, headers=None, **keyword_headers):
        """
        Unsubscribe from a destination by its unique identifier

        :param str id: the unique identifier to unsubscribe from
        :param dict headers: additional headers to send with the unsubscribe
        :param keyword_headers: additional headers to send with unsubscribe
        """
        assert id is not None, "'id' is required"
        headers = StompUtils.merge_headers([headers, keyword_headers])
        headers[StompUtils.HDR_ID] = id
        return self.send_frame(StompUtils.CMD_UNSUBSCRIBE, headers)
