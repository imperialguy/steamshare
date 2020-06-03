from decimal import (
    InvalidOperation,
    Decimal
)
from steamshare.utils.errors import LoggingError
from contextlib import contextmanager
from flask import jsonify
from dateutil import tz
import datetime
import tornado.ioloop
import tempfile
import logging
import pandas
import random
import sys
import os


class StaticShared(object):
    @staticmethod
    @contextmanager
    def environment(params):
        orig = os.environ.copy()
        os.environ.update(params)
        yield
        os.environ = orig

    @staticmethod
    def get_logger(filename,
                    formatter=None,
                    log_level=logging.DEBUG,
                    log_to_file=False,
                    log_file_name=None
                    ):
        if log_to_file and not log_file_name:
            raise LoggingError('Missing parameter: log_file_name')

        if not formatter:
            formatter = '[%(asctime)s] [p%(process)s] {%(pathname)s:%(lineno)d} [%(levelname)s] %(message)s'

        # xformatter = logging.Formatter(formatter)
        logging.basicConfig(format=formatter)
        xlogger = logging.getLogger(filename)

        handler = logging.FileHandler(log_file_name) if log_to_file and \
                    log_file_name else logging.StreamHandler(sys.stdout)
        # handler.set_name(filename)
        # handler.setLevel(log_level)
        # handler.setFormatter(xformatter)

        # xlogger.addHandler(handler)
        xlogger.setLevel(log_level)
        # xlogger.setFormatter(xformatter)

        return xlogger


class ClassicShared(object):
    def __init__(self, logger):
        self.logger = logger

    def run_loop(self, callback):
        loop = tornado.ioloop.IOLoop.current()
        loop.add_callback(callback)
        try:

            loop.start()
        except KeyboardInterrupt:
            self.logger.debug('interrupted - so exiting!')

    def generate_response(self, data, is_error=False, message=None, code=None):
        resp = {'data': data, 'success': not is_error}

        if is_error:
            resp['error'] = message
        else:
            resp['message'] = message

        resp = jsonify(resp)
        if is_error:
            resp.status_code = code

        return resp

    def get_current_timestamp(self, timestamp_format=None, to_zone=None,
                                from_zone=None):
        from_zone = tz.tzutc() if not from_zone else tz.gettz(from_zone)
        to_zone = tz.tzlocal() if not to_zone else tz.gettz(to_zone)

        current_timestamp = datetime.datetime.utcnow().replace(
            tzinfo=from_zone).astimezone(to_zone)

        if timestamp_format:
            return current_timestamp.strftime(timestamp_format)

        return current_timestamp

    def extract_digits(self, input):
        if isinstance(input, int):
            return input

        input = '' if input is None else input
        digits_only = ''.join(i for i in input if i.isdigit())

        return int(digits_only) if digits_only else 0

    def result_set_builder(self, result_set, columns, drop_columns=[]):
        """ Return database output as a dataframe

        :param result_set: output of database call
        :param columns: list of columns
        :param drop_columns: list of columns
        :type result_set: list
        :type columns: list
        :type drop_columns: list
        :returns: dataframe
        :rtype: pandas.DataFrame

        """
        dataframe = pandas.DataFrame(result_set)

        if dataframe.empty:
            return dataframe

        dataframe.columns = columns
        if drop_columns:
            dataframe = dataframe.drop(columns=drop_columns)

        return dataframe

    def random_pick(self, iplist):
        return iplist[random.randint(0, len(iplist) - 1)]

    def merge_and_partition_list(self, large_list, partition_size):
        """ Partition data for writing to database

        :param large_list: data in list
        :param partition_size: character limit in partition
        :type large_list: list
        :type partition_size: int
        :returns: List of partitioned data
        :rtype: list

        """
        partitioned_list = []
        partition = ""

        for item in large_list:
            if len(partition) + len(item) + 1 < partition_size:
                partition = partition + "," + item
            else:
                partitioned_list.append(partition)
                partition = item

        partitioned_list.append(partition)

        return partitioned_list

    def precision_converter(self, value):
        """ Convert value to Decimal and then to float to preserve precision

        :param value: Target value
        :type value: any
        :returns: The target value converted to a float
        :rtype: float

        """
        try:
            return Decimal(value)
        except InvalidOperation:
            return value

    def delete_dir(self, dir_path):
        """ Delete dir if exist

        :returns: None
        :rtype: NoneType

        """
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)

    def get_files(self, root_dir, file_type=None):
        """Get files

        :returns: A list of paths of the files
        :rtype: list

        :example:
        get_files("/tmp/files/inputs", ".csv")

        """
        if file_type:
            return [os.path.join(r, f) for r, _, files in os.walk(root_dir
            ) for f in files if f.endswith(file_type)]

        return [os.path.join(
            r, f) for r, _, files in os.walk(root_dir) for f in files]

    def get_temp_dir(self, sub_dir=''):
        """Get temp directory

        :returns: The path where outputs are temporarily written to
        :rtype: str

        """
        return os.path.join(tempfile.gettempdir(), sub_dir)
