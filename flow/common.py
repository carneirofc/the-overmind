#!/usr/bin/env python3
import redis
import logging
import time
import types


def log_config():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s',
                        datefmt='%Y-%m-%d,%H:%M:%S')


logger = logging.getLogger()


class RedisManager:
    _pool = None

    def __init__(self, stream_name, tick: float = 0.001, upstream_timeout=1):
        self.connection = None
        self.connect()

        self.downstream_data = stream_name + '#down#data'
        self.upstream_data = stream_name + '#up#data'
        self.upstream_listen = stream_name + '#up#listen'

        self._downstream_action = None
        self._pipeline = self.connection.pipeline(transaction=True)
        self._tick = tick

        self._upstream_listen_code = None
        self._upstream_timeout = upstream_timeout

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

    def master_downstream_handler(self, data: bytes):
        # Send stuff to redis
        self._upstream_listen_code = time.time()
        self._pipeline.delete(self.upstream_data)  # Remove old repose
        self._pipeline.set(self.upstream_listen, self._upstream_listen_code)  # Update listen code
        self._pipeline.set(self.downstream_data, data)  # Set downstream data
        self._pipeline.publish(self.upstream_listen, self._upstream_listen_code)  # Publish listen code
        self._pipeline.execute()
        logger.debug('{}: {}\t{}: {}'.format(
            self.downstream_data, data,
            self.upstream_listen, self._upstream_listen_code))

    def slave_upstream_listen(self, downstream_action: types.FunctionType):
        self._downstream_action = downstream_action
        p = self.connection.pubsub()

        p.subscribe(self.upstream_listen)
        logger.info('Initializing the subscribe event loop.')

        message = None
        while True:
            message = p.get_message(ignore_subscribe_messages=True)
            if message:
                self.slave_downstream_handler(message)
            time.sleep(self._tick)

    def slave_downstream_handler(self, _message_id):

        message_id = _message_id['data']
        downstream_data = self.connection.eval('''
            -- KEYS[1] self.redis_upstream_listen
            -- KEYS[2] self.redis_downstream_data
            -- KEYS[3] self.message_id

            if redis.call('exists', KEYS[2]) == 0 then
                return nil
            end

            if redis.call('get',  KEYS[1]) == KEYS[3] then
                return redis.call('get', KEYS[2])
            end

            return nil
            ''', 3, self.upstream_listen, self.downstream_data, message_id)

        if not downstream_data:
            logger.info('Timeout {}: {}'.format(self.upstream_listen, message_id))
            return

        logger.debug('{}: {}'.format(self.downstream_data, downstream_data))

        os_data = self._downstream_action(downstream_data)

        if os_data:
            res = self.connection.eval(
                '''
                -- KEYS[1] self.redis_upstream_data
                -- KEYS[2] os_data
                -- KEYS[3] self.redis_upstream_listen
                -- KEYS[4] message_id

                local valid_id = redis.call('get', KEYS[3]) == KEYS[4]

                if not valid_id then
                   return -1
                end

                if redis.call('exists', KEYS[1]) == 1 then
                    return -2
                end

               redis.call('set', KEYS[1], KEYS[2])
               return 1
                ''', 4, self.upstream_data, os_data, self.upstream_listen, message_id)

            logger.debug('{}: {} action_status={}'.format(self.upstream_data, os_data, res))
