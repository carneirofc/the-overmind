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
                 serial_use_terminator: bool = False,
                 serial_write_timeout: float = 2,
                 serial_bsmp: bool = False):

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
        self.serial_bsmp = serial_bsmp
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

    def data_handler(self, data: bytes, settings={}):
        res = []
        if not self.ser:
            self.connect()
        try:
            self.ser.reset_input_buffer()
            self.ser.reset_output_buffer()
            self.ser.write(data)

            ser_continue = True

            operation_timeout = settings['ReplyTimeout']/1000 if 'ReplyTimeout' in settings else self.serial_operation_timeout
            read_timeout = settings['ReadTimeout']/1000 if 'ReadTimeout' in settings else self.serial_read_timeout
            max_input = settings['MaxInput'] if 'MaxInput' in settings else -1
            terminator = list(settings['Terminator'].encode('utf-8')) if 'Terminator' in settings else self.serial_read_terminator
            terminator_len = len(terminator)

            self.ser.timeout = read_timeout

            tini = time.time()
            while ser_continue:
                b = self.ser.read(1)

                if b == b'':
                    ser_continue = False
                    logger.debug('Ser: Read timeout')

                if time.time() - tini > operation_timeout:
                    ser_continue = False
                    logger.debug('Ser: Operation timeout')

                res.append(b)

                if len(res) == max_input:
                    ser_continue = False
                    logger.debug('Ser: MaxInput')

                if self.serial_use_terminator and len(res) >= len(terminator):
                    ser_continue = res[-terminator_len:] == terminator
                    logger.debug('Ser: Terminator')

        except termios.error:
            logger.exception('Serial exception, closing connection.')
            self.ser.close()
            self.ser = None

        return b''.join(res)
