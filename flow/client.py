#!/usr/bin/env python3
import argparse
import configparser
import logging
import time
import asyncio
import common
from threading import RLock

from random import random

to_upstream_command = """
if redis.call('exists', KEYS[2]) == 1 and redis.call('exists', KEYS[1]) == 0 then 
  redis.call('set', KEYS[1], KEYS[3])
  redis.call('del', KEYS[2])
  return 1
else
  return -1
end"""

logger = logging.getLogger()


class BaseClient:
    def __init__(self, redis_manager: common.RedisManager, stream_name: str, client_id: str, **kwargs):
        self.client_id = client_id
        self.redis_manager = redis_manager
        self.redis_conn = redis_manager.connection

        self.redis_downstream_data = stream_name + '#down#data'
        self.redis_upstream_data = stream_name + '#up#data'
        self.redis_upstream_listen = stream_name + '#up#listen'

        self.message_lock = RLock()
        self.message_id = None

    def update_message_id(self, message_id):
        with self.message_lock:
            self.message_id = message_id

    def check_message(self, message_id):
        with self.message_lock:
            eq = self.message_id == message_id
        return eq

    def start(self):
        p = self.redis_conn.pubsub()

        p.subscribe(self.redis_upstream_listen)
        logger.info('Initializing the subscribe event loop.')

        while True:
            # Event Loop ?!
            self.downstream_handler(p.get_message(ignore_subscribe_messages=True))
            time.sleep(0.001)

    def downstream_handler(self, _message_id):
        if not _message_id:
            # No data from downstream !
            return

        self.message_id = _message_id['data']
        downstream_data = self.redis_conn.eval('''
            if redis.call('get',  KEYS[1]) == KEYS[3] then
                return redis.call('get', KEYS[2])
            end
            
            return nil
            ''', 3, self.redis_upstream_listen, self.redis_downstream_data, self.message_id)

        if not downstream_data:
            logger.info('Timeout {}: {}'.format(self.redis_upstream_listen, self.message_id))
            return

        logger.debug('{}: {}'.format(self.redis_downstream_data, downstream_data))

        num = random()

        os_data = None
        if downstream_data == b'1':
            os_data = '{}_{}_1\n'.format(self.client_id, num).encode('utf-8')
        elif downstream_data == b'2':
            pass
        elif downstream_data == b'3':
            os_data = '{}_{}_3\n'.format(self.client_id, num).encode('utf-8')
        else:
            os_data = '{}_NACK\n'.format(self.client_id)

        time.sleep(num)

        if os_data:
            res = self.redis_conn.eval(
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
                ''', 4, self.redis_upstream_data, os_data, self.redis_upstream_listen, self.message_id)

            logger.info('{}: {} action_status={}'.format(self.redis_upstream_data, os_data, res))


if __name__ == '__main__':
    cfg_parser = configparser.ConfigParser()
    with open('config.ini') as _f:
        cfg_parser.read_file(_f)

    parser = argparse.ArgumentParser("Client side - Pipeline connection")
    parser.add_argument('--stream-name', type=str)
    parser.add_argument('--client-id', type=str)

    common.log_config()

    args = parser.parse_args()
    if not args.client_id:
        client_id = 'cli_1'
    else:
        client_id = args.client_id

    params = {
        'stream_name': args.stream_name if args.stream_name else cfg_parser['DEFAULT']['stream_name'],
    }

    redis_cfg = cfg_parser['redis']
    common.RedisManager.init_pool(
        redis_ip=redis_cfg.get('redis_ip'),
        redis_port=redis_cfg.getint('redis_port'),
        redis_db=redis_cfg.getint('redis_db'))

    redis_manager = common.RedisManager()
    common.log_config()

    client = BaseClient(redis_manager=redis_manager, client_id=client_id, **params)
    client.start()
