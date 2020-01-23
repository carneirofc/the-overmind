#!/usr/bin/env python3

import logging
import os
import socket
import configparser
import argparse
import common

logger = logging.getLogger()


class BaseMaster:

    def __init__(self, redis_manager: common.RedisManager):
        self.redis_manager = redis_manager

    def get_from_device(self, *args, **kwargs):
        logger.warning("Override method {} from {}".format(self.get_from_device.__name__, self.__str__()))
        return b''

    def send_to_device(self, *args, **kwargs):
        logger.warning("Override method {} from {}".format(self.send_to_device.__name__, self.__str__()))
        return b''

    def start(self):
        while True:
            data = self.get_from_device()

            upstream_response = self.redis_manager.master_sync_send_receive(data)

            self.send_to_device(upstream_response)


class DGRAMSocketMaster(BaseMaster):
    CFG_DGRAM_SOCKET = 'DGRAM_socket'

    def __init__(self, socket_path,
                 redis_manager: common.RedisManager,
                 socket_reconnect_interval: int = 30,
                 socket_terminator: bytes = b'\n',
                 socket_buffer: int = 1,
                 socket_timeout: int = 5,
                 socket_use_terminator: bool = True,
                 socket_read_payload_length: bool = False):
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
        self.socket_use_terminator = socket_use_terminator

    def send_to_device(self, upstream_response):
        if upstream_response is None:
            upstream_response = b'TOUT\n'

        if type(upstream_response) != bytes:
            data = upstream_response.encode('utf-8')
        logger.debug('To device: {}'.format(upstream_response))
        self.conn.sendall(upstream_response)

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


if __name__ == '__main__':
    cfg_parser = configparser.ConfigParser()
    with open('config.ini') as _f:
        cfg_parser.read_file(_f)

    parser = argparse.ArgumentParser("IOC side - Pipeline connection")
    parser.add_argument('--socket-path', type=str)
    parser.add_argument('--stream-name', type=str)

    common.log_config()

    args = parser.parse_args()

    stream_name = args.stream_name if args.stream_name else cfg_parser['DEFAULT'].get('stream_name')
    socket_path = args.socket_path if args.socket_path else cfg_parser['DEFAULT'].get('socket_path')

    redis_cfg = cfg_parser['redis']
    common.RedisManager.init_pool(
        ip=redis_cfg.get('ip'),
        port=redis_cfg.getint('port'),
        db=redis_cfg.getint('db'))

    redis_manager = common.RedisManager(
        stream_name=stream_name,
        upstream_timeout=redis_cfg.getint('upstream_timeout'))

    cfg = cfg_parser[DGRAMSocketMaster.CFG_DGRAM_SOCKET]

    DGRAMSocketMaster(redis_manager=redis_manager,
                      socket_path=socket_path,
                      socket_read_payload_length=cfg.getboolean('read_payload_length'),
                      socket_reconnect_interval=cfg.getfloat('reconnect_interval'),
                      socket_terminator=cfg.get('terminator') if cfg.get('terminator') != 'LF' else b'\n',
                      socket_timeout=cfg.getfloat('timeout', 5),
                      socket_use_terminator=cfg.getboolean('use_terminator'),
                      socket_buffer=cfg.getint('buffer', 1)
                      ).start()
