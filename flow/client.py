#!/usr/bin/env python3
import argparse
import logging
import redis
import time

from random import random

lpush_size1 = """
local l = redis.call('get', KEYS[1])
if not l then 
  redis.call('set', KEYS[1], KEYS[2])
  redis.call('publish', KEYS[1], KEYS[2])
  return 1
else
  return l
end"""

downstream = None
upstream = None

redis_conn: redis.client = None


def redis_init():
    global redis_conn
    redis_conn = redis.Redis(host='localhost', port=6379, db=0)


def log_config():
    global logger
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s',
                        datefmt='%Y-%m-%d,%H:%M:%S')
    logger = logging.getLogger()


def downstream_handler(message):
    if not message:
        return

    data = message['data']
    logger.info("From downstream: {}".format(data))

    output = None
    if data == b'1':
        output = '{}_1\n'.format(random()).encode('utf-8')
    elif data == b'2':
        pass
    elif data == b'3':
        output = '{}_3\n'.format(random()).encode('utf-8')
    else:
        output = b'NACK\n'

    if output:

        logger.info('To upstream: {} REDIS {}'.format(output, redis_conn.eval(lpush_size1, 2, 'ioc_up', output)))

    #    redis_conn.publish('ioc_up', output)


if __name__ == '__main__':
    log_config()
    redis_init()
    p = redis_conn.pubsub()
    p.subscribe(**{'ioc_down': downstream_handler})

    while True:
        # Event Loop ?!
        downstream_handler(p.get_message(ignore_subscribe_messages=True, timeout=2))
        time.sleep(0.001)
