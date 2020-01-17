#!/usr/bin/env python3

import logging
import os
import socket
import configparser
import argparse
import common
import time

logger = logging.getLogger()

# LOCK_ON = '1'
# LOCK_OFF = '2'
#

class MakeBelive:

    def __init__(self, socket_path, stream_name: str, redis_manager: common.RedisManager,
                 redis_upstream_timeout: int=1, socket_reconnect_interval: int = 30,
                 socket_terminator: bytes = b'\n',
                 socket_buffer: int = 1, socket_timeout: int = 5, socket_use_terminator: bool = True,
                 socket_read_payload_length: bool = False, **kwargs):
        """

        :param socket_path:
        :param stream_name:
        :param redis_conn:
        :param redis_upstream_timeout:
        :param socket_reconnect_interval:
        :param socket_terminator:
        :param socket_buffer:
        :param socket_timeout:
        :param socket_use_terminator:
        :param socket_read_payload_length: If enabled, the first 4 bytes are the remaining payload length.
        """
        self.socket_path = socket_path
        self.socket_buffer = socket_buffer
        self.socket_terminator = socket_terminator
        self.socket_reconnect_interval = socket_reconnect_interval
        self.socket_timeout = socket_timeout
        self.socket_use_terminator = socket_use_terminator
        self.socket_read_payload_length = socket_read_payload_length
        self.redis_downstream = stream_name + '#down'
        self.redis_upstream = stream_name + '#up'
        self.redis_upstream_listen = stream_name + '#up#listen'

        if redis_upstream_timeout <= 0:
            logger.error('Redis upstream timeout must be greater than zero. Using default value of 5.')
            self.redis_upstream_timeout = 5
        else:
            self.redis_upstream_timeout = redis_upstream_timeout

        self.redis_conn = redis_manager.connection
        # self.redis_pubsub = self.redis_conn.pubsub()
        self.redis_pipe = self.redis_conn.pipeline()

    def __str__(self):
        return 'The Overmind sees! Socket {}, reconnect interval {}, buffer {}, terminator {}. Redis upstream {}, ' \
               'downstream {}'.format(self.socket_path, self.socket_reconnect_interval, self.socket_buffer,
                                      self.socket_terminator, self.redis_downstream, self.redis_upstream)

    @staticmethod
    def upstream_handler(conn: socket.socket, data):
        if data is None:
            data = b'TOUT\n'

        logger.debug('From upstream: {}'.format(data))
        if type(data) != bytes:
            data = data.encode('utf-8')
        conn.sendall(data)

    def read_from_socket(self, conn: socket.socket):
        data = b''
        socket_continue_recv = True
        while socket_continue_recv:
            try:
                if self.socket_read_payload_length:
                    payload_length = int(conn.recv(4))
                    b = conn.recv(payload_length)
                    if b == b'':
                        break
                    data = b
                    socket_continue_recv = False
                else:
                    b = conn.recv(self.socket_buffer)
                    if b == b'':
                        break
                    data += b
                    if self.socket_use_terminator and data.endswith(self.socket_terminator):
                        data = data[:-len(self.socket_terminator)]
                        socket_continue_recv = False
            except socket.timeout:
                socket_continue_recv = False
                logger.debug('Socket read operation terminated via timeout, data {}.'.format(data))
        return data



    def start(self):
        if os.path.exists(self.socket_path):
            os.remove(self.socket_path)

        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:

            s.bind(self.socket_path)
            s.listen()
            logger.info('Unix Socket {}: Waiting for a connection'.format(self.socket_path))

            while True:
                conn, addr = s.accept()
                conn.setblocking(True)
                conn.settimeout(self.socket_timeout)
                try:
                    with conn:
                        logger.info('Connected to the unix socket {} {} {}'.format(self.socket_path, conn, addr))
                        # self.redis_pubsub.subscribe(self.redis_upstream)
                        while True:
                            data = self.read_from_socket(conn)

                            if not data or data == b'':
                                logger.info('No data received from the unix socket {}'.format(self.socket_path))
                                break

                            # Send stuff to redis
                            with self.redis_conn.pipeline() as redis_pipe:
                                redis_pipe.multi()
                                redis_pipe.delete(self.redis_upstream)
                                redis_pipe.publish(self.redis_downstream, data)
                                redis_pipe.set(self.redis_upstream_listen, '1')
                                redis_pipe.expire(self.redis_upstream_listen, self.redis_upstream_timeout)
                                redis_pipe.execute()
                            logger.debug('{}: {}'.format(self.redis_downstream, data))
                            # self.redis_pubsub.subscribe(self.redis_upstream)
                            # logger.debug('Sub {}'.format(self.redis_upstream))

                            # self.redis_pubsub.get_message(ignore_subscribe_messages=True,
                            #                               timeout=self.redis_upstream_timeout)

                            while self.redis_conn.exists(self.redis_upstream_listen):
                                time.sleep(0.01)

                            # self.redis_pubsub.unsubscribe(self.redis_upstream)
                            # logger.debug('USub {}'.format(self.redis_upstream))
                            # self.redis_pipe.multi()
                            upstream_response = self.redis_conn.get(self.redis_upstream)
                            # self.redis_pipe.set(self.redis_upstream_listen, LOCK_ON)
                            # upstream_response = self.redis_pipe.execute()[0]  # First response

                            self.upstream_handler(conn, upstream_response)

                except:
                    logger.exception('The connection with the unix socket {} has been closed.'.format(self.socket_path))
                    # self.redis_pubsub.unsubscribe(self.redis_upstream)


if __name__ == '__main__':
    cfg_parser = configparser.ConfigParser()
    with open('config.ini') as _f:
        cfg_parser.read_file(_f)

    parser = argparse.ArgumentParser("IOC side - Pipeline connection")
    parser.add_argument('--socket-path', type=str)
    parser.add_argument('--stream-name', type=str)

    common.log_config()

    args = parser.parse_args()

    params = {
        'stream_name': args.stream_name if args.stream_name else cfg_parser['DEFAULT']['stream_name'],
        'socket_path': args.socket_path if args.socket_path else cfg_parser['DEFAULT']['socket_path'],
    }

    redis_cfg = cfg_parser['redis']
    common.RedisManager.init_pool(
        redis_ip=redis_cfg.get('redis_ip'),
        redis_port=redis_cfg.getint('redis_port'),
        redis_db=redis_cfg.getint('redis_db'))

    redis_manager = common.RedisManager()

    blivin = MakeBelive(redis_manager=redis_manager, **params)
    blivin.start()
