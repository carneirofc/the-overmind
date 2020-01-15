#!/usr/bin/env python3
import logging
import os
import socket
import redis
import argparse

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


class MakeBelive:

    def __init__(self, socket_path, redis_stream: str, redis_conn: redis.client,
                 redis_upstream_timeout: float = 5, socket_reconnect_interval: int = 30,
                 socket_terminator: bytes = b'\n',
                 socket_buffer: int = 1, socket_timeout: int = 5, socket_use_terminator: bool = True,
                 socket_read_payload_length: bool = False):
        """

        :param socket_path:
        :param redis_stream:
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
        self.redis_upstream_timeout = redis_upstream_timeout
        self.redis_downstream = redis_stream + '_down'
        self.redis_upstream = redis_stream + '_up'
        self.redis_lock = redis_stream + '_lock'
        self.redis_conn = redis_conn
        self.redis_pubsub = redis_conn.pubsub()

    def __str__(self):
        return 'The Overmind sees! Socket {}, reconnect interval {}, buffer {}, terminator {}. Redis upstream {}, ' \
               'downstream {}'.format(self.socket_path, self.socket_reconnect_interval, self.socket_buffer,
                                      self.socket_terminator, self.redis_downstream, self.redis_upstream)

    def upstream_handler(self, conn: socket.socket, data):
        if data is None:
            data = b'TOUT\n'

        logger.debug('From upstream: {}'.format(data))
        if type(data) != bytes:
            data = data.encode('utf-8')
        conn.sendall(data)

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
                self.redis_pubsub.subscribe(self.redis_upstream)
                try:
                    with conn:
                        logger.info('Connected to the unix socket {} {} {}'.format(self.socket_path, conn, addr))
                        while True:
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

                            if not data or data == b'':
                                logger.info('No data received from the unix socket {}'.format(self.socket_path))
                                break

                            # Send stuff to redis
                            self.redis_conn.eval("redis.call('del', KEYS[1]) redis.call('publish', KEYS[1], KEYS[2]) return 1",
                                2, self.redis_downstream, data)
                            #self.redis_conn.push(self.redis_lock, 0)
                            #self.redis_conn.publish(self.redis_downstream, data)
                            logger.info('To downstream: {}'.format(data))

                            if self.redis_pubsub.get_message(ignore_subscribe_messages=True, timeout=self.redis_upstream_timeout):
                                self.upstream_handler(conn, self.redis_conn.lpop(self.redis_upstream))
                            else:
                                self.upstream_handler(conn, None)
                            self.redis_conn.set(self.redis_downstream, b'closed')

                except:
                    logger.exception('The connection with the unix socket {} has been closed.'.format(self.socket_path))
                    self.redis_pubsub.unsubscribe(self.redis_upstream)


if __name__ == '__main__':
    parser = argparse.ArgumentParser("IOC side - Pipeline connection")
    parser.add_argument('--socket-path', default='/tmp/socket', type=str)
    parser.add_argument('--stream-name', default='test', type=str)

    log_config()
    redis_init()

    args = parser.parse_args()

    MakeBelive(socket_path=args.socket_path, redis_stream=args.stream_name, redis_conn=redis_conn).start()
