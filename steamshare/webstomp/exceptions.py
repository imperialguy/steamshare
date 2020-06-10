"""Errors thrown by stomp.py connections.

"""

class ErrorFrameReceivedException(Exception):
    """
    Common exception class. All specific stomp.py exceptions are subclasses
    of StompException, allowing the library user to catch all current and
    future library exceptions.
    """

class EmptyFrameException(Exception):
    """
    Common exception class. All specific stomp.py exceptions are subclasses
    of StompException, allowing the library user to catch all current and
    future library exceptions.
    """

class StompException(Exception):
    """
    Common exception class. All specific stomp.py exceptions are subclasses
    of StompException, allowing the library user to catch all current and
    future library exceptions.
    """


class ConnectionClosedException(StompException):
    """
    Raised in the receiver thread when the connection has been closed
    by the server.
    """


class NotConnectedException(StompException):
    """
    Raised when there is currently no server connection.
    """


class ConnectFailedException(StompException):
    """
    Raised by Connection.attempt_connection when reconnection attempts
    have exceeded Connection.__reconnect_attempts_max.
    """

class DisconnectFailedException(StompException):
    """
    Raised by wait_for_disconnection.
    """

class AcknowledgeFailedException(StompException):
    """
    Raised by wait_for_acknowledgement.
    """

class InterruptedException(StompException):
    """
    Raised by receive when data read is interrupted.
    """
