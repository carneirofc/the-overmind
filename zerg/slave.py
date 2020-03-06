#!/usr/bin/env python3
import logging
import serial
import time
import termios
import os

import zerg.common

logger = logging.getLogger()


class BaseSlave:
    """ Base slave object for synchronous communication. """

    def __init__(self, redis_manager: zerg.common.RedisManager, client_id: str, priority: str = 'high'):
        self.client_id = client_id
        self.client_id_encoded = self.client_id.encode('utf-8')

        self.redis_manager = redis_manager
        self.redis_manager.slave_priority = priority

    def start(self):
        self.redis_manager.slave_alive_signal_start()
        self.redis_manager.slave_upstream_listen(downstream_action=self.downstream_action)

    def downstream_action(self, downstream_data):
        logger.warning(
            "Pass another function to method {} from {}".format(self.downstream_action.__name__, self.__str__()))
        return self.client_id_encoded + b"#" + downstream_data + b'\n'


class SerialSlave(BaseSlave):
    def __init__(self, redis_manager: zerg.common.RedisManager, client_id: str, priority: str,
                 serial_device: str,
                 serial_baudrate: int,
                 serial_buffer: int = 256,
                 serial_operation_timeout: float = 1.25,
                 serial_read_terminator=None,
                 serial_read_timeout: float = 0.5,
                 serial_write_timeout: float = 2):

        super().__init__(redis_manager, client_id, priority)

        self.serial_write_timeout = serial_write_timeout
        self.serial_baudrate = serial_baudrate
        self.serial_device = serial_device
        self.serial_operation_timeout = serial_operation_timeout
        if serial_read_terminator:
            self.serial_read_terminator = list(serial_read_terminator)
            self.serial_read_terminator_len = len(self.serial_read_terminator)
        else:
            self.serial_read_terminator = None
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

    def downstream_action(self, data: bytes, settings={}):
        res = []
        if not self.ser:
            self.connect()
        try:
            self.ser.reset_input_buffer()
            self.ser.reset_output_buffer()
            self.ser.write(data)

            ser_continue = True

            operation_timeout = settings[
                                    'ReplyTimeout'] / 1000 if 'ReplyTimeout' in settings else self.serial_operation_timeout
            read_timeout = settings['ReadTimeout'] / 1000 if 'ReadTimeout' in settings else self.serial_read_timeout
            max_input = settings['MaxInput'] if 'MaxInput' in settings else -1

            serial_use_terminator = ('Terminator' in settings) or (self.serial_read_terminator is not None)
            if serial_use_terminator:
                terminator = list(settings['Terminator'].encode('utf-8')) if 'Terminator' in settings else self.serial_read_terminator
                terminator_len = len(terminator)

            self.ser.timeout = read_timeout

            tini = time.time()
            while ser_continue:
                b = self.ser.read(1)

                if b == b'':
                    ser_continue = False
                    logger.warning('Ser: Read timeout')

                if time.time() - tini > operation_timeout:
                    ser_continue = False
                    logger.warning('Ser: Operation timeout')

                res.append(b)

                if len(res) == max_input:
                    ser_continue = False
                    logger.debug('Ser: MaxInput')

                if serial_use_terminator and len(res) >= len(terminator):
                    ser_continue = res[-terminator_len:] == terminator
                    logger.debug('Ser: Terminator')

        except termios.error:
            logger.exception('Serial exception, closing connection.')
            self.ser.close()
            self.ser = None

        return b''.join(res)
