#!/usr/bin/env python3
import logging
import os
import socket
import redis

downstream = None
upstream = None

def redis_init():
    global redis_conn
    redis_conn = redis.Redis(host='localhost', port=6379, db=0)
    upstream.pubsub()
    upstream.subscribe('upstream')

def log_config():
    global logger
    logging.basicConfig(level=logging.INFO, format='%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s',
                        datefmt='%Y-%m-%d,%H:%M:%S')
    logger = logging.getLogger()


class TcpUnixBind():

    def __init__(self, socket_path, socket_buffer, reconnect_interval):
        self.socket_path = socket_path
        self.socket_buffer = socket_buffer
        self.reconnect_interval = reconnect_interval
        self.connected_to_server = False

    def __str__(self):
        return 'The Overmind sees! Socket {}, reconnect interval {}, buffer {}'.format(self.socket_path,
                                                                                       self.reconnect_interval,
                                                                                       self.socket_buffer)

    def start(self):
        if os.path.exists(self.socket_path):
            os.remove(self.socket_path)

        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.bind(self.socket_path)
            s.listen()
            logger.info('Unix Socket {}: Waiting for a connection'.format(self.socket_path))

            while True:
                conn, addr = s.accept()
                try:
                    with conn:
                        logger.info('Connected to the unix socket {} {} {}'.format(self.socket_path, conn, addr))
                        while True:
                            data = conn.recv(self.socket_buffer)

                            if not data:
                                logger.info('No data received from the unix socket {}'.format(self.socket_path))
                                break

                            # Send stuff to redis
                            print(data)
                except:
                    logger.exception('The connection with the unix socket {} has been closed.'.format(self.socket_path))


if __name__ == '__main__':
    log_config()
