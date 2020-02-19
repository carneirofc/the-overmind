#!/usr/bin/env python3

import logging
import os
import socket

import zerg.common
import zerg

logger = logging.getLogger()


class BaseMaster:

    def __init__(self, redis_manager: zerg.common.RedisManager):
        self.redis_manager = redis_manager

    def get_from_device(self, *args, **kwargs):
        logger.warning("Override method {} from {}".format(self.get_from_device.__name__, self.__str__()))
        return b''

    def send_to_device(self, *args, **kwargs):
        logger.warning("Override method {} from {}".format(self.send_to_device.__name__, self.__str__()))
        return b''

    def start(self):
        while True:
            settings = b'{}'
            data = self.get_from_device()
            if data.startswith(b'CFG|') and data.endswith(b'|GFC'):
                settings = data.split(b'|')[1]
                data = self.get_from_device()

            upstream_response = self.redis_manager.master_sync_send_receive(data, settings=settings)

            self.send_to_device(upstream_response)


class STREAMSocketMaster(BaseMaster):
    CFG_STREAM_SOCKET = 'STREAM_socket'

    def __init__(self, socket_path,
                 redis_manager: zerg.common.RedisManager,
                 socket_reconnect_interval: int = 30,
                 socket_terminator: bytes = b'\n',
                 socket_buffer: int = 1,
                 socket_timeout: int = 5,
                 socket_use_terminator: bool = True,
                 socket_read_payload_length: bool = False,
                 socket_trim_terminator: bool = True
                 ):
        """

        :param socket_path:
        :param socket_reconnect_interval:
        :param socket_terminator:
        :param socket_buffer:
        :param socket_timeout:
        :param socket_use_terminator:
        :param socket_read_payload_length: If enabled, the first 4 bytes are the remaining payload length.
        """
        super().__init__(redis_manager)

        self.socket_path = socket_path
        self.socket_buffer = socket_buffer
        self.socket_read_payload_length = socket_read_payload_length
        self.socket_reconnect_interval = socket_reconnect_interval
        self.socket_terminator = socket_terminator
        self.socket_timeout = socket_timeout
        self.socket_trim_terminator = socket_trim_terminator
        self.socket_use_terminator = socket_use_terminator

    def send_to_device(self, upstream_response):
        if upstream_response is None:
            upstream_response = b'TOUT'

        if type(upstream_response) != bytes:
            upstream_response = upstream_response.encode('utf-8')
        logger.debug('To device: {}'.format(upstream_response))

        self.conn.sendall(upstream_response + self.socket_terminator)

    def get_from_device(self, *args, **kwargs):

        data = b''
        socket_continue_recv = True
        while socket_continue_recv:
            try:
                if self.socket_read_payload_length:
                    payload_length = int(self.conn.recv(4))
                    b = self.conn.recv(payload_length)
                    if b == b'':
                        break
                    data = b
                    socket_continue_recv = False
                else:
                    b = self.conn.recv(self.socket_buffer)
                    if b == b'':
                        break
                    data += b
                    if self.socket_use_terminator and data.endswith(self.socket_terminator):
                        if self.socket_trim_terminator:
                            data = data[:-len(self.socket_terminator)]
                        socket_continue_recv = False
            except socket.timeout:
                socket_continue_recv = False
                logger.debug('Socket read operation terminated via timeout, data {}.'.format(data))
        return data

    def start(self):
        logger.info(self.__str__())
        if os.path.exists(self.socket_path):
            os.remove(self.socket_path)

        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:

            s.bind(self.socket_path)
            s.listen()
            logger.info('Unix Socket {}: Waiting for a connection'.format(self.socket_path))

            while True:
                self.conn, addr = s.accept()
                self.conn.setblocking(True)
                self.conn.settimeout(self.socket_timeout)
                try:
                    with self.conn:
                        logger.info('Connected to the unix socket {} {} {}'.format(self.socket_path, self.conn, addr))
                        super().start()
                except:
                    logger.exception('The connection with the unix socket {} has been closed.'.format(self.socket_path))
