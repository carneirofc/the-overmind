#!/usr/bin/env python3
import argparse
import configparser
import logging
import redis
import time
import common

from random import random

# self.redis_upstream self.redis_upstream_listen data
to_upstream_command2 = """
return redis.call('exists', 'KEYS[1]') == 0 """
to_upstream_command = """
if redis.call('exists', 'KEYS[1]') == 0 then 
  redis.call('set', KEYS[1], KEYS[3])
  redis.call('del', KEYS[2])
  return 1
else
  return -1
end"""

logger = logging.getLogger()


class BaseClient:
    def __init__(self, redis_manager: common.RedisManager, stream_name: str, client_id:str, **kwargs):
        self.client_id = client_id
        self.redis_manager = redis_manager
        self.redis_conn = redis_manager.connection
        self.redis_downstream = stream_name + '#down'
        self.redis_upstream = stream_name + '#up'
        self.redis_upstream_listen = stream_name + '#up#listen'

    def start(self):
        p = self.redis_conn.pubsub()

        p.subscribe(**{self.redis_downstream: self.downstream_handler})
        logger.info('Initializing the subscribe event loop.')

        while True:
            # Event Loop ?!
            self.downstream_handler(p.get_message(ignore_subscribe_messages=True))
            time.sleep(0.001)

    def downstream_handler(self, message):
        if not message:
            return

        downstream_data = message['data']
        logger.debug('{}: {}'.format(self.redis_downstream, downstream_data))

        outputstream_data = None
        if downstream_data == b'1':
            outputstream_data = '{}_{}_1\n'.format(self.client_id, random()).encode('utf-8')
        elif downstream_data == b'2':
            pass
        elif downstream_data == b'3':
            outputstream_data = '{}_{}_3\n'.format(self.client_id, random()).encode('utf-8')
        else:
            outputstream_data= '{}_NACK\n'.format(self.client_id)

        if outputstream_data:
            logger.info('{}: {} action_status={}'.format(
                self.redis_upstream, outputstream_data, self.redis_conn.eval(
                    to_upstream_command,
                    3,
                    self.redis_upstream,
                    self.redis_upstream_listen,
                    outputstream_data)))


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
