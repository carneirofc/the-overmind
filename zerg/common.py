#!/usr/bin/env python3
import ast
import logging
import redis
import time
import types
import requests
import netifaces
import ipaddress
import threading

COMM_TYPE = b'SERIAL'
HOSTS = [
    '10.0.38.42', '10.0.38.46', '10.0.38.59',
    '10.128.255.5', '10.128.255.4', '10.128.255.3'
]
LOCATION = '/download/cons-config'
BEAGLE = '/beagle.json'
APPLICATION = '/app.json'

HIGH = 'high'
LOW = 'low'
EXPIRE_TIMER = 2

VALID_NETWORKS = [
    ipaddress.IPv4Network('10.128.0.0/16'),
    ipaddress.IPv4Network('10.0.38.0/24'),
    ipaddress.IPv4Network('10.0.6.0/24'),
]


def log_config(level: int = logging.INFO):
    logging.basicConfig(level=level, format='%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s',
                        datefmt='%Y-%m-%d,%H:%M:%S')


logger = logging.getLogger()


class BeagleConfig:
    def __init__(self, config: dict, ip: str):
        self.ip = ip
        self.app = config['app']
        self.priority = config['priority']
        self.endpoint = config['endpoint']


def get_device_settings():
    url = None
    for host in HOSTS:
        try:
            url = 'http://{}{}{}'.format(host, LOCATION, BEAGLE)
            return requests.get(url=url, verify=False).json()
        except requests.exceptions.ConnectionError:
            logger.warning('Unable to get response from {}'.format(url))

    return None


def get_config_settings():
    url = None
    for host in HOSTS:
        try:
            url = 'http://{}{}{}'.format(host, LOCATION, APPLICATION)
            return requests.get(url=url, verify=False).json()
        except requests.exceptions.ConnectionError:
            logger.warning('Unable to get response from {}'.format(url))

    return None


def get_interfaces_data():
    """ Get all interface settings """
    data = []
    for iface in netifaces.interfaces():
        addresses = netifaces.ifaddresses(iface)
        for v in addresses.values():
            for d in v:
                data.append(d)
    return data


def get_beagle_config():
    """ Get a single config related to this ip list """
    ips = get_valid_ips()
    configs = get_device_settings()
    config = None
    for ip in ips:
        if ip in configs:
            config = configs[ip]
            break
    return BeagleConfig(config, ip)


def get_application_config(app: str):
    """ Get application config """
    return get_config_settings()[app]


def get_valid_ips():
    """ Find valid ips """
    ips = []
    for v in get_interfaces_data():
        if 'netmask' in v:
            try:
                ipv4 = ipaddress.IPv4Address(v['addr'])
                for net in VALID_NETWORKS:
                    if ipv4 in net:
                        ips.append(str(ipv4))
                        break
            except:
                logger.info('Invalid interface {}'.format(v))
    return ips


def get_log_level(level: str):
    if level == 'notset':
        return 0
    elif level == 'debug':
        return 10
    elif level == 'info':
        return 20
    elif level == 'warning':
        return 30
    elif level == 'error':
        return 40
    elif level == 'critical':
        return 50
    else:
        return 10


def get_terminator_bytes(terminator: str):
    """ Parse ascii symbolic to byte code. """
    return terminator \
        .replace('\\NULL\\', '\x00') \
        .replace('\\SOH\\', '\x01') \
        .replace('\\STX\\', '\x02') \
        .replace('\\ETX\\', '\x03') \
        .replace('\\EOT\\', '\x04') \
        .replace('\\ENQ\\', '\x05') \
        .replace('\\ACK\\', '\x06') \
        .replace('\\BEL\\', '\x07') \
        .replace('\\BS\\', '\x08') \
        .replace('\\TAB\\', '\x09') \
        .replace('\\HT\\', '\x09') \
        .replace('\\LF\\', '\n') \
        .replace('\\VT\\', '\x0B') \
        .replace('\\FF\\', '\x0C') \
        .replace('\\NP\\', '\x0C') \
        .replace('\\CR\\', '\r') \
        .replace('\\SO\\', '\x0E') \
        .replace('\\SI\\', '\x0F') \
        .replace('\\DLE\\', '\x10') \
        .replace('\\DC1\\', '\x11') \
        .replace('\\DC2\\', '\x12') \
        .replace('\\DC3\\', '\x13') \
        .replace('\\DC4\\', '\x14') \
        .replace('\\NAK\\', '\x15') \
        .replace('\\SYN\\', '\x16') \
        .replace('\\ETB\\', '\x17') \
        .replace('\\CAN\\', '\x18') \
        .replace('\\EM\\', '\x19') \
        .replace('\\SUB\\', '\x1A') \
        .replace('\\ESC\\', '\x1B') \
        .replace('\\FS\\', '\x1C') \
        .replace('\\GS\\', '\x1D') \
        .replace('\\RS\\', '\x1E') \
        .replace('\\US\\', '\x1F') \
        .replace('\\DEL\\', '\x7F') \
        .encode('utf-8')


class RedisManager:
    _pool = None

    def __init__(self, stream_name, ip='localhost', port=6379, db=0, tick: float = 0.001, upstream_timeout: float = 1, reconnect_interval: float = 30,
                 slave_priority: str = HIGH):

        RedisManager.init_pool(ip, port, db)

        self.slave_alive_thread = threading.Thread(target=self.slave_alive_worker, daemon=True)
        self.connection = None
        self.connect()

        self.slave_priority = slave_priority

        self.downstream_data = stream_name + '#down#data'
        self.upstream_data = stream_name + '#up#data'
        self.upstream_listen = stream_name + '#up#listen'
        self.slave_status = stream_name + '#slave'

        # This is a redis hash containing special settings for comm
        self.device_comm_settings = stream_name + '#device#comm#settings'

        self._downstream_action = None
        self._pipeline = self.connection.pipeline(transaction=True)
        self._tick = tick
        self._reconnect_interval = reconnect_interval

        self._upstream_listen_code = None

        if upstream_timeout <= 0.:
            logger.error('Redis upstream timeout must be greater than zero. Using default value of 2.')
            self._upstream_timeout = 2.
        else:
            self._upstream_timeout = upstream_timeout

    def slave_alive_signal_start(self):
        self.slave_alive_thread.start()

    def slave_alive_worker(self):
        """ Refresh slave status """
        worker_connection = redis.Redis(connection_pool=RedisManager._pool)
        while True:
            time.sleep(1)
            worker_connection.eval('''
                -- KEYS[1] self.slave_status
                -- KEYS[2] self.slave_priority
                -- KEYS[3] HIGH
                -- KEYS[4] LOW
                -- KEYS[5] EXPIRE_TIMER
                
                -- return 0 ok
                
                local slaveStatus = redis.call('get', KEYS[1])
                
                -- If is nil the slave this client assumes no matter what
                if slaveStatus == nil then
                    redis.call('set', KEYS[1], KEYS[2])
                    return 0
                end
               
                -- If not nil, check if I'm the HIGH priority. If so, set the status
                if KEYS[2] == KEYS[3] then
                    redis.call('setex', KEYS[1], KEYS[2], KEYS[5])
                    return 0
                end
                
                -- LOW priority only set if the slave is nil
                return -1
                
            ''', 6, self.slave_status, self.slave_priority, HIGH, LOW, EXPIRE_TIMER)

    @staticmethod
    def init_pool(ip: str = 'localhost', port: int = 6379, db: int = 0):
        if not RedisManager._pool:
            RedisManager._pool = redis.ConnectionPool(host=ip, port=port, db=db)
            logger.info('Redis pool: {}:{} db={}'.format(ip, port, db))
        else:
            logger.warning('Redis pool already exists.')

    def connect(self):
        if RedisManager._pool:
            self.connection = redis.Redis(connection_pool=RedisManager._pool)
            logger.info('Redis connection from pool.')
        else:
            logger.error('Redis pool not initialized.')

    def disconnect(self):
        if self.connection:
            self.connection.close()
            logger.info('Redis connection closed.')

    def master_sync_send_receive(self, data, settings: bytes = b'{}'):
        """
        :@param settings: String encoded dictionary that will populate the settings key
        :@param data: Payload
        """
        try:
            self.master_downstream_handler(data, settings)
            self.master_pool_data()
            return self.master_upstream_handler()
        except redis.exceptions.ConnectionError:
            logger.fatal('Redis connection lost to {}.'.format(RedisManager._pool.__str__()))
            return None

    def master_pool_data(self):
        while not self.connection.exists(self.upstream_data) and \
                (time.time() - self._upstream_listen_code) < self._upstream_timeout:
            time.sleep(self._tick)

    def master_upstream_handler(self):
        # Get stuff from redis
        self._pipeline.delete(self.downstream_data)
        self._pipeline.delete(self.upstream_listen)
        self._pipeline.get(self.upstream_data)
        return self._pipeline.execute()[2]

    def master_downstream_handler(self, data: bytes, settings: bytes = b'{}'):
        # Send stuff to redis
        self._upstream_listen_code = time.time()
        self._pipeline.delete(self.upstream_data)  # Remove old repose
        self._pipeline.set(self.upstream_listen, self._upstream_listen_code)  # Update listen code
        self._pipeline.set(self.downstream_data, data)  # Set downstream data
        self._pipeline.publish(self.upstream_listen, self._upstream_listen_code)  # Publish listen code
        self._pipeline.set(self.device_comm_settings, settings)  # Send device settings
        self._pipeline.execute()
        logger.debug('{}: {}\t{}: {}'.format(
            self.downstream_data, data,
            self.upstream_listen, self._upstream_listen_code))

    def slave_upstream_listen(self, downstream_action: types.FunctionType):
        while True:
            try:
                self._downstream_action = downstream_action
                p = self.connection.pubsub()

                p.subscribe(self.upstream_listen)
                logger.info('Initializing the subscribe event loop.')

                while True:
                    message = p.get_message(ignore_subscribe_messages=True)
                    if message:
                        self.slave_downstream_handler(message)
                    time.sleep(self._tick)
            except redis.exceptions.ConnectionError:
                logger.fatal('Redis connection lost to {}. Retry in {} seconds.'.format(RedisManager._pool.__str__(),
                                                                                        self._reconnect_interval))
                time.sleep(self._reconnect_interval)

    def slave_downstream_handler(self, _message_id):

        message_id = _message_id['data']
        self._pipeline.eval('''
            -- KEYS[1] self.redis_upstream_listen
            -- KEYS[2] self.redis_downstream_data
            -- KEYS[3] self.message_id
            -- KEYS[4] self.slave_status
            -- KEYS[5] self.slave_priority
            
            if redis.call('exists', KEYS[2]) == 0 then
               return nil
            end
            
            local status = redis.call('get', KEYS[4])
            
            -- If the current status is not my priority, abort !
            if status != KEYS[5] then
                return nil
            end
            
            -- If there's no response from downstream and this request is still valid
            if redis.call('get',  KEYS[1]) == KEYS[3] then
                return redis.call('get', KEYS[2])
            end

            return nil
            ''', 7, self.upstream_listen, self.downstream_data, message_id, self.device_comm_settings,
                            self.slave_status, self.slave_priority)
        self._pipeline.get(self.device_comm_settings)
        response = self._pipeline.execute()
        downstream_data = response[0]
        try:
            settings = ast.literal_eval(response[1].decode('utf-8'))
        except:
            settings = {}
            logger.debug("Impossible to parse {}.".format(response[1]))

        if not downstream_data:
            logger.info('Timeout {}: {}'.format(self.upstream_listen, message_id))
            return

        logger.debug('{}: {}'.format(self.downstream_data, downstream_data))

        os_data = self._downstream_action(downstream_data, settings)

        if os_data:
            res = self.connection.eval(
                '''
                -- KEYS[1] self.redis_upstream_data
                -- KEYS[2] os_data
                -- KEYS[3] self.redis_upstream_listen
                -- KEYS[4] message_id

                local valid_id = redis.call('get', KEYS[3]) == KEYS[4]

                -- If the message is deprecated exit
                if not valid_id then
                   return -1
                end
                
                -- If there is another answer exit
                if redis.call('exists', KEYS[1]) == 1 then
                    return -2
                end

                -- Answer the request
                redis.call('set', KEYS[1], KEYS[2])
                return 1
                ''', 4, self.upstream_data, os_data, self.upstream_listen, message_id)

            logger.debug('{}: {} action_status={}'.format(self.upstream_data, os_data, res))
