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
# import multiprocessing.managers
# import multiprocessing_logging
import multiprocessing
import threading
import traceback
import tempfile
import logging
import inspect
import pathlib
import pandas
import random
import ctypes
import trio
import sys
import os

# backup_autoproxy = multiprocessing.managers.AutoProxy
#
# # Defining a new AutoProxy that handles unwanted key argument 'manager_owned'
# def redefined_autoproxy(token, serializer, manager=None, authkey=None,
#           exposed=None, incref=True, manager_owned=True):
#     # Calling original AutoProxy without the unwanted key argument
#     return backup_autoproxy(token, serializer, manager, authkey,
#                      exposed, incref)
#
# # Updating AutoProxy definition in multiprocessing.managers package
# multiprocessing.managers.AutoProxy = redefined_autoproxy
# multiprocessing_logging.install_mp_handler()


class NestedDict(dict):
    """Nested Dictionary class

    """

    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value


class SteamProcess(multiprocessing.Process):
    def __init__(self, *args, **kwargs):
        multiprocessing.Process.__init__(self, *args, **kwargs)
        self._pconn, self._cconn = multiprocessing.Pipe()
        self._exception = None

    def run(self):
        try:
            multiprocessing.Process.run(self)
            self._cconn.send(None)
        except Exception as e:
            tb = traceback.format_exc()
            self._cconn.send((tb))
            raise e  # You can still rise this exception if you need to

    @property
    def exception(self):
        if self._pconn.poll():
            self._exception = self._pconn.recv()
        return self._exception


class SteamThread(threading.Thread):
    def _get_my_tid(self):
        """determines this (self's) thread id"""
        if not self.isAlive():
            raise threading.ThreadError("the thread is not active")

        # do we have it cached?
        if hasattr(self, "_thread_id"):
            return self._thread_id

        # no, look for it in the _active dict
        for tid, tobj in threading._active.items():
            if tobj is self:
                self._thread_id = tid
                return tid

        raise AssertionError("could not determine the thread's id")

    def raise_exc(self, exctype):
        """raises the given exception type in the context of this thread"""
        # StaticShared._async_raise(self._get_my_tid(), exctype)
        if not self.isAlive():
            raise threading.ThreadError("the thread is not active")
        # print('Thread id: {} name: {}'.format(self.ident, self.name))
        StaticShared._async_raise(self.ident, exctype)

    def terminate(self):
        """raises SystemExit in the context of the given thread, which should
        cause the thread to exit silently (unless caught)"""
        self.raise_exc(SystemExit)


class StaticShared(object):
    PROJ_ROOT = str(pathlib.Path(__file__).parent.parent)
    FIXTURES_ROOT = os.path.join(PROJ_ROOT, 'fixtures')
    RESOURCES_ROOT = os.path.join(PROJ_ROOT, 'resources')

    @staticmethod
    @contextmanager
    def environment(params):
        orig = os.environ.copy()
        os.environ.update(params)
        yield
        os.environ = orig

    @staticmethod
    def _async_raise(tid, exctype):
        """raises the exception, performs cleanup if needed"""
        if not inspect.isclass(exctype):
            raise TypeError("Only types can be raised (not instances)")
        # res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid,
        # ctypes.py_object(exctype))
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_long(tid),
            ctypes.py_object(exctype))
        if res == 0:
            raise ValueError("invalid thread id")
        elif res != 1:
            # """if it returns a number greater than one, you're in trouble,
            # and you should call it again with exc=NULL to revert the
            # effect"""
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, 0)
            raise SystemError("PyThreadState_SetAsyncExc failed")

    @staticmethod
    def load_resource(filename, engine=None):
        """ Load resource file

        :param filename: name of the resource file - needs to yaml filetype
        :param engine: name of the engine if applicable
        :type engine: str
        :type filename: str
        :returns: the corresponding yaml data
        :rtype: ObjectDict

        """
        resource_path = os.path.join(RESOURCES_ROOT, engine, filename
                                     ) if engine else os.path.join(RESOURCES_ROOT, filename)

        return ObjectDict(dataloader(resource_path))

    @staticmethod
    def sequence_generator():
        n = 1
        while True:
            yield n
            n += 1

    @staticmethod
    def load_config(engine):
        """ Load configuration based on the engine

        :param engine: name of the engine if applicable
        :type engine: str
        :returns: engine configuration
        :rtype: ObjectDict

        """
        config = load_resource('config.yaml', engine)
        bconfig = load_resource('base.yaml')
        config.update(bconfig)

        return config

    @staticmethod
    def get_logger(filename,
                   formatter=None,
                   log_level=logging.DEBUG,
                   log_to_file=False,
                   log_file_name=None,
                   log_file_mode='a'
                   ):
        if log_to_file and not log_file_name:
            raise LoggingError('Missing parameter: log_file_name')

        if not formatter:
            formatter = '[%(asctime)s] [p%(process)s] {%(pathname)s:%(lineno'\
                ')d} [%(levelname)s] %(message)s'

        # xformatter = logging.Formatter(formatter)
        logging.basicConfig(format=formatter)
        xlogger = logging.getLogger(filename)

        handler = logging.FileHandler(log_file_name, mode=log_file_mode
                                      ) if log_to_file and log_file_name else \
            logging.StreamHandler(sys.stdout)
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

    def write_to_file(self, filepath, message, override=False):
        # root_dir = os.path.join(os.sep, 'tmp', 'logs', getpass.getuser())
        # filepath = os.path.join(root_dir, filename)
        mode = 'w' if override else 'a+'
        with open(filepath, mode) as f:
            f.write(message)
            f.write('\n')

    def run_loop(self, callback):
        loop = tornado.ioloop.IOLoop.current()
        loop.add_callback(callback)
        try:

            loop.start()
        except KeyboardInterrupt:
            self.logger.debug('interrupted - so exiting!')

    def zero_padded(self, number, padding=4):
        str_num = str(number)
        actual_padding = padding - len(str_num)
        padded_num = ''.join((''.join(('0' for _ in range(actual_padding))),
                              str_num))
        self.logger.debug('Input number: {}, padding: {}, paddded number: '
                          '{}'.format(number, padding, padded_num))
        return padded_num

    def generate_response(self, data, is_error=False, message=None,
                          code=None):
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
