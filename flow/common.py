#!/usr/bin/env python3
import redis
import logging


def log_config():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s',
                        datefmt='%Y-%m-%d,%H:%M:%S')


logger = logging.getLogger()


class RedisManager:
    _pool = None

    def __init__(self, redis_ip: str = 'localhost', redis_port: int = 6379, redis_db: int = 0):
        self.redis_ip = redis_ip
        self.redis_db = redis_db
        self.redis_port = redis_port
        self.connection = None
        self.connect()

    @staticmethod
    def init_pool(redis_ip: str = 'localhost', redis_port: int = 6379, redis_db: int = 0):
        if not RedisManager._pool:
            RedisManager._pool = redis.ConnectionPool(host=redis_ip, port=redis_port, db=redis_db)
            logger.info('Redis pool: {}:{} db={}'.format(redis_ip, redis_port, redis_db))
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
