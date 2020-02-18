#!/usr/bin/env python3
import logging
import serial
import time
import termios
import os
import types

import zerg.common

logger = logging.getLogger()

class BaseSlave:
    """
    Base slave object for synchronous communication.
    """

    def __init__(self, redis_manager: zerg.common.RedisManager, client_id: str):
        self.client_id = client_id
        self.client_id_encoded = self.client_id.encode('utf-8')

        self.redis_manager = redis_manager

    def start(self):
        self.redis_manager.slave_upstream_listen(self.data_handler)

    def data_handler(self, downstream_data):
        logger.warning("Pass another function to method {} from {}".format(self.data_handler.__name__, self.__str__()))
        return self.client_id_encoded + b"#" + downstream_data + b'\n'


class SerialSlave(BaseSlave):
    def __init__(self, redis_manager: zerg.common.RedisManager, client_id: str,
                 serial_device: str,
                 serial_baudrate: int,
                 serial_buffer: int = 256,
                 serial_operation_timeout: float = 1.25,
                 serial_read_terminator: bytes = b'\r\n',
                 serial_read_timeout: float = 0.5,
                 serial_write_timeout: float = 2,
                 serial_use_terminator: bool = False):

        super().__init__(redis_manager, client_id)

        self.serial_write_timeout = serial_write_timeout
        self.serial_baudrate = serial_baudrate
        self.serial_device = serial_device
        self.serial_operation_timeout = serial_operation_timeout
        self.serial_read_terminator = list(serial_read_terminator)
        self.serial_read_terminator_len = len(self.serial_read_terminator)
        self.serial_use_terminator = serial_use_terminator
        self.serial_read_timeout = serial_read_timeout
        self.serial_buffer = serial_buffer
        self.ser = None

    def start(self):
        self.connect()
        super().start()

    def connect(self, retry=True):
        if retry:
            while not os.path.exists(self.serial_device):
                logger.error('Serial device \'{}\' does not exists. Retry in 30s.'.format(self.serial_device))
                time.sleep(30)

        logger.info('Connecting to serial device.')
        self.ser = serial.Serial(
            self.serial_device,
            self.serial_baudrate,
            timeout=self.serial_read_timeout,
            write_timeout=self.serial_write_timeout)

    def data_handler(self, data: bytes):
        res = []
        if not self.ser:
            self.connect()
        try:
            self.ser.reset_input_buffer()
            self.ser.reset_output_buffer()
            self.ser.write(data)

            ser_continue = True

            tini = time.time()
            while ser_continue:
                b = self.ser.read(self.serial_buffer)
                if b == b'':
                    ser_continue = False
                    logger.debug('Ser: Read timeout')
                if time.time() - tini > self.serial_operation_timeout:
                    ser_continue = False
                    logger.debug('Ser: Operation timeout')

                res.append(b)

                if self.serial_use_terminator and len(res) >= len(self.serial_read_terminator):
                    ser_continue = res[-self.serial_read_terminator_len:] == self.serial_read_terminator
                    logger.debug('Ser: Terminator')

        except termios.error:
            logger.exception('Serial exception, closing connection.')
            self.ser.close()
            self.ser = None

        return b''.join(res)
