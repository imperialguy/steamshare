""" General utility functions.

"""
import uuid
import copy
import re


class StompFrame(object):
    """
    A STOMP frame (or message).

    :param str cmd: the protocol command
    :param dict headers: a map of headers for the frame
    :param body: the content of the frame.

    """

    def __init__(self, cmd, headers=None, body=None):
        self.cmd = cmd
        self.headers = headers if headers is not None else {}
        self.original_headers = copy.copy(self.headers)
        self.body = body

    def __str__(self):
        return "{cmd=%s,headers=[%s],body=%s}" % (self.cmd, self.headers,
                                                  self.body)


class StompUtils(object):
    HDR_ACCEPT_VERSION = "accept-version"
    HDR_ACK = "ack"
    HDR_CONTENT_LENGTH = "content-length"
    HDR_CONTENT_TYPE = "content-type"
    HDR_DESTINATION = "destination"
    HDR_HEARTBEAT = "heart-beat"
    HDR_HOST = "host"
    HDR_ID = "id"
    HDR_MESSAGE_ID = "message-id"
    HDR_LOGIN = "login"
    HDR_PASSCODE = "passcode"
    HDR_RECEIPT = "receipt"
    HDR_SUBSCRIPTION = "subscription"
    HDR_TRANSACTION = "transaction"

    CMD_ABORT = "ABORT"
    CMD_ACK = "ACK"
    CMD_BEGIN = "BEGIN"
    CMD_COMMIT = "COMMIT"
    CMD_CONNECT = "CONNECT"
    CMD_DISCONNECT = "DISCONNECT"
    CMD_NACK = "NACK"
    CMD_STOMP = "STOMP"
    CMD_SEND = "SEND"
    CMD_SUBSCRIBE = "SUBSCRIBE"
    CMD_UNSUBSCRIBE = "UNSUBSCRIBE"

    NULL = b'\x00'

    ##
    # Used to parse STOMP header lines in the format "key:value",
    #
    HEADER_LINE_RE = re.compile("(?P<key>[^:]+)[:](?P<value>.*)")

    ##
    # As of STOMP 1.2, lines can end with either line feed, or carriage return
    # plus line feed.
    #
    PREAMBLE_END_RE = re.compile(b"\n\n|\r\n\r\n")

    ##
    # As of STOMP 1.2, lines can end with either line feed, or carriage return
    # plus line feed.
    #
    LINE_END_RE = re.compile("\n|\r\n")

    ##
    # Used to replace the "passcode" to be dumped in the transport log (at
    # debug level)
    #
    PASSCODE_RE = re.compile(r"passcode:\s+[^\' ]+")

    _HEADER_ESCAPES = {
        '\r': '\\r',
        '\n': '\\n',
        ':': '\\c',
        '\\': '\\\\',
    }
    _HEADER_UNESCAPES = dict((value, key) for (key, value) in
                             _HEADER_ESCAPES.items())

    HEARTBEAT_FRAME = StompFrame("heartbeat")

    @staticmethod
    def decode(byte_data, encoding="utf-8"):
        """
        Decode the byte data to a string if not None.

        :param bytes byte_data: the data to decode

        :rtype: str
        """
        if byte_data is None:
            return None
        return byte_data.decode(encoding, errors="replace")

    @staticmethod
    def encode(char_data, encoding="utf-8"):
        """
        Encode the parameter as a byte string.

        :param char_data: the data to encode

        :rtype: bytes
        """
        if type(char_data) is str:
            return char_data.encode(encoding, errors="replace")
        elif type(char_data) is bytes:
            return char_data
        else:
            raise TypeError('message should be a string or bytes, found %s'
                            '' % type(char_data))

    @staticmethod
    def pack(pieces=()):
        """
        Join a sequence of strings together.

        :param list pieces: list of strings

        :rtype: bytes
        """
        return b''.join(pieces)

    @staticmethod
    def join(chars=()):
        """
        Join a sequence of characters into a string.

        :param bytes chars: list of chars

        :rtype: str
        """
        return b''.join(chars).decode()

    @staticmethod
    def is_eol_default(c):
        return c == b'\x0a'

    @staticmethod
    def _unescape_header(matchobj):
        escaped = matchobj.group(0)
        unescaped = StompUtils._HEADER_UNESCAPES.get(escaped)
        if not unescaped:
            # TODO: unknown escapes MUST be treated as fatal protocol error per spec
            unescaped = escaped
        return unescaped

    @staticmethod
    def parse_headers(lines, offset=0):
        """
        Parse the headers in a STOMP response

        :param list(str) lines: the lines received in the message response
        :param int offset: the starting line number

        :rtype: dict(str,str)
        """
        headers = {}
        for header_line in lines[offset:]:
            header_match = StompUtils.HEADER_LINE_RE.match(header_line)
            if header_match:
                key = header_match.group("key")
                key = re.sub(r'\\.', StompUtils._unescape_header, key)
                if key not in headers:
                    value = header_match.group("value")
                    value = re.sub(r'\\.', StompUtils._unescape_header, value)
                    headers[key] = value
        return headers

    @staticmethod
    def parse_frame(frame):
        """
        Parse a STOMP frame into a StompFrame object.

        :param bytes frame: frame received from the server (as a byte string)

        :rtype: StompFrame
        """
        mat = StompUtils.PREAMBLE_END_RE.search(frame)
        if mat:
            preamble_end = mat.start()
            body_start = mat.end()
        else:
            preamble_end = len(frame)
            body_start = preamble_end
        preamble = StompUtils.decode(frame[0:preamble_end])
        preamble_lines = StompUtils.LINE_END_RE.split(preamble)
        preamble_len = len(preamble_lines)
        body = frame[body_start:]

        # Skip any leading newlines
        first_line = 0
        while first_line < preamble_len and len(preamble_lines[first_line
                                                               ]) == 0:
            first_line += 1

        if first_line >= preamble_len:
            return None

        # Extract frame type/command
        cmd = preamble_lines[first_line]

        # Put headers into a key/value map
        headers = StompUtils.parse_headers(preamble_lines, first_line + 1)

        return StompFrame(cmd, headers, body)

    @staticmethod
    def merge_headers(header_map_list):
        """
        Helper function for combining multiple header maps into one.

        :param list(dict) header_map_list: list of maps

        :rtype: dict
        """
        headers = {}
        for header_map in header_map_list:
            if header_map:
                headers.update(header_map)
        return headers

    @staticmethod
    def clean_headers(headers):
        rtn = headers
        if "passcode" in headers:
            rtn = copy.copy(headers)
            rtn["passcode"] = "********"
        return rtn

    # lines: lines returned from a call to convert_frames
    @staticmethod
    def clean_lines(lines):
        return re.sub(StompUtils.PASSCODE_RE, "passcode:********", str(lines))

    @staticmethod
    def calculate_heartbeats(shb, chb):
        """
        Given a heartbeat string from the server, and a heartbeat tuple from
        the client, calculate what the actual heartbeat settings should be.

        :param (str,str) shb: server heartbeat numbers
        :param (int,int) chb: client heartbeat numbers

        :rtype: (int,int)
        """
        (sx, sy) = shb
        (cx, cy) = chb
        x = 0
        y = 0
        if cx != 0 and sy != '0':
            x = max(cx, int(sy))
        if cy != 0 and sx != '0':
            y = max(cy, int(sx))
        return x, y

    @staticmethod
    def convert_frame(frame):
        """
        Convert a frame to a list of lines separated by newlines.

        :param StompFrame frame: the StompFrame object to convert

        :rtype: list(str)
        """
        lines = []

        body = None
        if frame.body:
            body = StompUtils.encode(frame.body)

            if StompUtils.HDR_CONTENT_LENGTH in frame.headers:
                frame.headers[StompUtils.HDR_CONTENT_LENGTH] = len(body)

        if frame.cmd:
            lines.append(StompUtils.encode(frame.cmd))
            lines.append(StompUtils.ENC_NEWLINE)
        for key, vals in sorted(frame.headers.items()):
            if vals is None:
                continue
            if type(vals) != tuple:
                vals = (vals,)
            for val in vals:
                lines.append(StompUtils.encode("%s:%s\n" % (key, val)))
        lines.append(StompUtils.ENC_NEWLINE)
        if body:
            lines.append(body)

        if frame.cmd:
            lines.append(StompUtils.ENC_NULL)
        return lines

    @staticmethod
    def length(s):
        """
        Null (none) safe length function.

        :param str s: the string to return length of (None allowed)

        :rtype: int
        """
        if s is not None:
            return len(s)
        return 0

    @staticmethod
    def get_uuid():
        return str(uuid.uuid4())

    @staticmethod
    def get_errno(e):
        """
        Return the errno of an exception, or the first argument if errno is not available.

        :param Exception e: the exception object
        """
        try:
            return e.errno
        except AttributeError:
            return e.args[0]

    ENC_NEWLINE = encode.__func__("\n")
    ENC_NULL = encode.__func__(NULL)
