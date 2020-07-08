from steamshare.utils.errors import RequestLauncherError
from steamshare.utils.core import ClassicCore
import pendulum
import urllib3
import json

urllib3.disable_warnings()


class RMQHTTPClient(object):
    def __init__(self, logger, root_url, vhost, user, password,
                 ssl_verify=False):
        self.logger = logger
        self.api_url = self.build_url(root_url, 'api')
        self.vhost = vhost
        self.user = user
        self.password = password
        self.ssl_verify = ssl_verify
        self.exchanges_base_url = self.form_url('exchanges')
        self.vhosts_base_url = self.form_url('vhosts')
        self.channels_base_url = self.form_url('channels')
        self.consumers_base_url = self.form_url('consumers')
        self.nodes_base_url = self.form_url('nodes')
        self.connections_base_url = self.form_url('connections')
        self.queues_base_url = self.form_url('queues')
        self.bindings_base_url = self.form_url('bindings')
        self.users_base_url = self.form_url('users')
        self.permissions_base_url = self.form_url('permissions')
        self.tpermissions_base_url = self.form_url('topic-permissions')
        self.policies_base_url = self.form_url('policies')
        self.healthchecks_base_url = self.form_url('healthchecks')

    def retrying_rmq_api_request(self,
                                 request_type,
                                 url,
                                 timeout=None,
                                 retries=None,
                                 backoff_factor=None,
                                 method_whitelist=None,
                                 status_forcelist=None,
                                 expected_status_code=200,
                                 data=None,
                                 headers=None,
                                 proxies=None,
                                 user=None,
                                 password=None,
                                 ssl_verify=False):

        timeout = 2.0
        retries = 1
        backoff_factor = 0.001
        # method_whitelist = method_whitelist or config.defaults.method_whitelist
        # status_forcelist = status_forcelist or config.defaults.status_forcelist
        authentication = (
            user,
            password
        )

        response = ClassicCore(self.logger).retrying_request(
            request_type, url, timeout, retries, backoff_factor,
            method_whitelist, status_forcelist, expected_status_code, data,
            headers, proxies, authentication, ssl_verify)

        if response.text:
            return json.loads(response.text)

    def form_url(self, tail):
        return self.build_url(self.api_url, tail)

    def build_url(self, *args):
        return '/'.join(args)

    def make_request(self, url, method, data=None):
        return self.retrying_rmq_api_request(method, url, user=self.user,
                                             password=self.password,
                                             ssl_verify=self.ssl_verify,
                                             data=data)

    @property
    def bindings(self):
        return self.make_request(self.bindings_base_url, 'GET')

    @property
    def permissions(self):
        return self.make_request(self.permissions_base_url, 'GET')

    @property
    def tpermissions(self):
        return self.make_request(self.tpermissions_base_url, 'GET')

    @property
    def nodes(self):
        return self.make_request(self.nodes_base_url, 'GET')

    @property
    def connections(self):
        return self.make_request(self.connections_base_url, 'GET')

    @property
    def channels(self):
        return self.make_request(self.channels_base_url, 'GET')

    @property
    def consumers(self):
        return self.make_request(self.consumers_base_url, 'GET')

    @property
    def vhosts(self):
        return self.make_request(self.vhosts_base_url, 'GET')

    @property
    def exchanges(self):
        return self.make_request(self.exchanges_base_url, 'GET')

    @property
    def queues(self):
        return self.make_request(self.queues_base_url, 'GET')

    @property
    def users(self):
        return self.make_request(self.users_base_url, 'GET')

    @property
    def wpusers(self):
        url = self.build_url(self.users_base_url, 'without-permissions')

        return self.make_request(self.url, 'GET')

    def create_permission(self, vhost=None, user=None, configure=True,
                          write=True, read=True):
        url = self.build_url(self.permissions_base_url, vhost if vhost else
                             self.vhost, user if user else self.user)

        data = {'configure': '.*' if configure else '',
                'write': '.*' if write else '',
                'read': '.*' if read else ''}

        return self.make_request(url, 'PUT', data=data)

    def create_tpermission(self, exchange, vhost=None, user=None,
                           configure=True, write=True, read=True):
        url = self.build_url(self.permissions_base_url, vhost if vhost else
                             self.vhost, user if user else self.user)

        data = {'configure': '.*' if configure else '',
                'write': '.*' if write else '',
                'read': '.*' if read else ''}

        return self.make_request(url, 'PUT', data=data)

    def create_vhost(self, name):
        url = self.build_url(self.vhosts_base_url, name)

        data = {
            'description': 'virtual host {}'.format(name),
            'tags': 'accounts,production'
        }

        return self.make_request(url, 'PUT', data=data)

    def create_user(self, name, password, manager=True, administrator=False,
                    monitor=False):
        url = self.build_url(self.users_base_url, name)
        tags = []
        if manager:
            tags.append('management')
        if administrator:
            tags.append('administrator')
        if monitor:
            tags.append('monitoring')
        tags = ','.join(tags)

        data = {'password': password,
                'tags': tags}

        return self.make_request(url, 'PUT', data=data)

    def create_topic_exchange(self, name, vhost=None):
        return self.create_exchange(vhost if vhost else self.vhost, name,
                                    'topic')

    def create_exchange(self, name, etype, alternate_exchange=None,
                        vhost=None):
        url = self.build_url(self.exchanges_base_url, vhost if vhost else
                             self.vhost, name)

        arguments = {'alternate-exchange': alternate_exchange
                     } if alternate_exchange else {}

        data = {'type': etype,
                'auto_delete': False,
                'durable': True,
                'internal': False,
                'arguments': arguments}

        return self.make_request(url, 'PUT', data=data)

    def get_eeq_bindings(self, btype, exchange, eorq, vhost=None):
        param = 'q' if btype == 'eq' else 'e'
        url = self.build_url(self.bindings_base_url, vhost if vhost else
                             self.vhost, 'e', exchange, param, eorq)

        return self.make_request(url, 'GET')

    def get_vhost_bindings(self, vhost=None):
        url = self.build_url(self.bindings_base_url, vhost if vhost else
                             self.vhost)

        return self.make_request(url, 'GET')

    def get_vhost_queues(self, vhost=None):
        url = self.build_url(self.queues_base_url, vhost if vhost else
                             self.vhost)

        return self.make_request(url, 'GET')

    def get_vhost_exchanges(self, vhost=None):
        url = self.build_url(self.exchanges_base_url, vhost if vhost else
                             self.vhost)

        return self.make_request(url, 'GET')

    def get_vhost_consumers(self, vhost=None):
        url = self.build_url(self.consumers_base_url, vhost if vhost else
                             self.vhost)

        return self.make_request(url, 'GET')

    def get_vhost_channels(self, vhost=None):
        url = self.build_url(self.vhosts_base_url, vhost if vhost else
                             self.vhost, 'channels')

        return self.make_request(url, 'GET')

    def get_vhost_connections(self, vhost=None):
        url = self.build_url(self.vhosts_base_url, vhost if vhost else
                             self.vhost, 'connections')

        return self.make_request(url, 'GET')

    def get_vhost_permissions(self, vhost=None):
        url = self.build_url(self.vhosts_base_url, vhost if vhost else
                             self.vhost, 'permissions')

        return self.make_request(url, 'GET')

    def get_user_permissions(self, user=None):
        url = self.build_url(self.users_base_url, user if user else
                             self.user, 'permissions')

        return self.make_request(url, 'GET')

    def get_user_topic_permissions(self, user=None):
        url = self.build_url(self.users_base_url, user if user else
                             self.user, 'topic-permissions')

        return self.make_request(url, 'GET')

    def get_connection_channels(self, conn_name):
        url = self.build_url(self.connections_base_url, conn_name, 'channels')

        return self.make_request(url, 'GET')

    def create_eeq_binding(self, btype, exchange, eorq, routing_key,
                           vhost=None, **kwargs):
        param = 'q' if btype == 'eq' else 'e'
        url = self.build_url(self.bindings_base_url, vhost if vhost else
                             self.vhost, 'e', exchange, param, eorq)
        data = {}
        if routing_key:
            data['routing_key'] = routing_key
        if kwargs:
            data['arguments'] = kwargs

        return self.make_request(url, 'POST', data=data)

    def view_eeq_binding(self, btype, exchange, eorq, routing_key,
                         vhost=None):
        param = 'q' if btype == 'eq' else 'e'
        url = self.build_url(self.bindings_base_url, vhost if vhost else
                             self.vhost, 'e', exchange, param, eorq, routing_key)

        return self.make_request(url, 'GET')

    def delete_eeq_binding(self, exchange, queue, routing_key, vhost=None):
        param = 'q' if btype == 'eq' else 'e'
        url = self.build_url(self.bindings_base_url, vhost if vhost else
                             self.vhost, 'e', exchange, param, eorq, routing_key)

        return self.make_request(url, 'DELETE')

    def create_queue(self, name, node=None, vhost=None):
        url = self.build_url(self.queues_base_url, vhost if vhost else
                             self.vhost, name)

        data = {'auto_delete': False,
                'durable': True,
                'arguments': {}
                }

        if node:
            data['node'] = node

        return self.make_request(url, 'PUT', data=data)

    def delete_queue(self, name, only_if_empty=True, vhost=None):
        url = self.build_url(self.queues_base_url, vhost if vhost else
                             self.vhost, name)

        data = {'if-empty': True} if only_if_empty else None

        return self.make_request(url, 'DELETE', data=data)

    def purge_queue(self, name, vhost=None):
        url = self.build_url(self.queues_base_url, vhost if vhost else
                             self.vhost, name, 'contents')

        try:
            self.make_request(url, 'DELETE')
        except RequestLauncherError as e:
            if e.code == 404:
                self.logger.debug('Queue {} doesn\'t exist'.format(name))
            else:
                raise

    def view_queue(self, name, vhost=None):
        url = self.build_url(self.queues_base_url, vhost if vhost else
                             self.vhost, name)

        return self.make_request(url, 'GET')

    def view_connection(self, conn_name):
        url = self.build_url(self.connections_base_url, conn_name)

        return self.make_request(url, 'GET')

    def view_channel(self, channel_name):
        url = self.build_url(self.channels_base_url, channel_name)

        return self.make_request(url, 'GET')

    def view_permission(self, user, vhost=None):
        url = self.build_url(self.permissions_base_url, vhost if vhost else
                             self.vhost, user)

        return self.make_request(url, 'GET')

    def view_node(self, node):
        url = self.build_url(self.nodes_base_url, node)

        return self.make_request(url, 'GET')

    def view_user(self, user=None):
        url = self.build_url(self.users_base_url, user if user else self.user)

        return self.make_request(url, 'GET')

    def view_exchange(self, exchange, vhost=None):
        url = self.build_url(self.exchanges_base_url, vhost if vhost else
                             self.vhost, exchange)

        return self.make_request(url, 'GET')

    def consume_queue(self, name, max_num_messages, requeue=False,
                      vhost=None):
        url = self.build_url(self.queues_base_url, vhost if vhost else
                             self.vhost, name, 'get')

        data = {'count': max_num_messages,
                'ackmode': 'ack_requeue_true' if requeue else
                'ack_requeue_false',
                'encoding': 'auto', 'truncate': 50000}

        return self.make_request(url, 'POST', data=data)

    def publish_to_exchange(self, exchange, routing_key, message,
                            time_formatter=False, vhost=None):
        url = self.build_url(self.exchanges_base_url, vhost if vhost else
                             self.vhost, exchange, 'publish')

        if time_formatter:
            formatter = pendulum.now("utc")
            message = '[{}] {}'.format(formatter, message)

        data = {'properties': {},
                'routing_key': routing_key,
                'payload': message,
                'payload_encoding': 'string'
                }

        resp = self.make_request(url, 'POST', data=data)

    def delete_connection(self, conn_name):
        url = self.build_url(self.connections_base_url, conn_name)

        self.make_request(url, 'DELETE')

    def delete_permission(self, user, vhost=None):
        url = self.build_url(self.permissions_base_url, vhost if vhost else
                             self.vhost, user)

        self.make_request(url, 'DELETE')

    def delete_vhost(self, vhost=None):
        url = self.build_url(self.vhosts_base_url, vhost if vhost else
                             self.vhost)

        self.make_request(url, 'DELETE')

    def delete_user(self, user=None):
        url = self.build_url(self.users_base_url, user if user else
                             self.user)

        self.make_request(url, 'DELETE')

    def delete_users(self, *users):
        url = self.build_url(self.users_base_url, 'bulk-delete')

        data = {'users': users}

        self.make_request(url, 'POST', data=data)
