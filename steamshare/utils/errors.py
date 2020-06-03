import json


class Error(Exception):
    """ General Error

    """

    def __init__(self, error_code, message):
        """
        :param msg: the error message.
        :type msg: string

        """
        self.code = error_code
        self.message = message


class ValidationError(Error):
    """ Validation Error

    """

    def __init__(self, error_code, val_errors):
        """
        :param val_errors: validation errors
        :type val_errors: dict

        """
        self.code = error_code
        self.message = json.dumps(val_errors)


class ServiceError(Error):
    pass


class InvalidRequestTypeError(Error):
    pass


class RequestLauncherError(Error):
    pass


class NoRollbackError(Error):
    pass

class LoggingError(Error):
    def __init__(self, msg):
        """
        :param msg: the error message.
        :type msg: string

        """
        self.msg = msg
