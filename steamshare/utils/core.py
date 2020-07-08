from steamshare.utils.errors import RequestLauncherError
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import requests
import re


class StaticCore(object):
    @staticmethod
    def get_state_tag(prefect_state):
        pattern = '^<(\w*): (.*)>$'
        matches = re.search(pattern, str(prefect_state))
        return next(iter(matches.groups())).upper()


class ClassicCore(object):
    def __init__(self, logger):
        self.logger = logger

    def requests_retry_session(self,
                               retries,
                               backoff_factor,
                               status_forcelist,
                               method_whitelist,
                               ssl_verify,
                               session=None):
        self.logger.debug('Building a requests retry session with the '
                          'following parameters\nretries: {}, backoff_factor: {}, '
                          'status_forcelist: {}, method_whitelist: {}, '
                          'ssl_verify: {}'.format(retries, backoff_factor,
                                                  status_forcelist, method_whitelist, ssl_verify))
        session = session or requests.Session()
        session.verify = ssl_verify
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            method_whitelist=method_whitelist,
            status_forcelist=status_forcelist,
        )

        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        return session

    def retrying_request(self,
                         request_type,
                         url,
                         timeout,
                         retries,
                         backoff_factor,
                         method_whitelist,
                         status_forcelist,
                         expected_status_code,
                         data=None,
                         headers=None,
                         proxies=None,
                         authentication=None,
                         ssl_verify=False,
                         is_json=True):
        request_params = dict()

        request_params.update(dict(timeout=timeout))

        if data:
            if is_json:
                request_params.update(dict(json=data))
            else:
                request_params.update(dict(data=data))
        if headers:
            request_params.update(dict(headers=headers))
        if proxies:
            request_params.update(dict(proxies=proxies))

        if authentication:
            request_params.update(dict(auth=authentication))

        session = self.requests_retry_session(retries,
                                              backoff_factor,
                                              status_forcelist,
                                              method_whitelist,
                                              ssl_verify)

        try:
            self.logger.debug('request_type: {} url: {}'.format(request_type,
                                                                url))
            response = session.request(request_type, url, **request_params)
        except Exception as e:
            self.logger.debug(
                'Unable to peform a {} request to {} due to:\n\n{}'.format(
                    request_type, url, e))

            raise RequestLauncherError(500, 'SteamShare Request Lanuch Error')
        else:
            if response.status_code >= 400:
                self.logger.debug('{} request to {} failed (status_code: {})'
                                  ' with the reason: {} and the following'
                                  ' unexpected response:\n\n{}'.format(
                                      request_type, url,
                                      response.status_code,
                                      response.reason,
                                      response.content))
                raise RequestLauncherError(response.status_code,
                                           response.reason
                                           )
            else:
                self.logger.debug('{} request to {} is successful with the'
                                  ' following response\n{}'.format(
                                      request_type, url, response.text))

            return response


class ObjectDict(dict):
    """ Provides dictionary with values also accessible by attribute

    Sample Usage:

    >>> p = {'a': 3, 'b': 4}
    >>> k = ObjectDict(p)
    >>> k.a
    3
    >>> k.b
    4

    """

    def __getattr__(self, attr):
        retval = self[attr]

        attr_reg_expr = re.compile(r'^(\w+)(\[)(\d+)(\])$')
        matches = re.findall(attr_reg_expr, attr)

        if matches:
            attr = matches[0][0]
            index = int(matches[0][2])
            retval = self[attr]

            if isinstance(retval, list):
                retval = retval[index]

        if isinstance(retval, dict):
            retval = ObjectDict(retval)

        return retval

    def __setattr__(self, attr, value):
        self[attr] = value

    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            # return None
            value = self[item] = type(self)()
            return value
