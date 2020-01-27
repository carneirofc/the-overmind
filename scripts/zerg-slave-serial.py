#!/usr/bin/env python3
import argparse
import configparser
import logging

import zerg.slave
import zerg.common

if __name__ == '__main__':
    logger = logging.getLogger()

    parser = argparse.ArgumentParser("Client side - Pipeline connection")
    parser.add_argument('--stream-name', type=str)
    parser.add_argument('--client-id', type=str, default='cli_1')
    parser.add_argument('--config-ini', type=str, default='config.ini', help='General configuration')
    parser.add_argument('--logging-level', type=str, default='info',
                        choices=['notset', 'debug', 'info', 'warning', 'error', 'critical'])
    parser.add_argument('--serial-ini', type=str, default='serial.ini', help='Serial comm configuration')

    args = parser.parse_args()

    zerg.common.log_config(
        level=zerg.common.get_log_level(args.logging_level))

    cfg_parser = configparser.ConfigParser()
    cfg_ini_path = zerg.get_abs_path(args.config_ini)
    logger.info('Loading config from {}'.format(cfg_ini_path))

    with open(cfg_ini_path) as _f:
        cfg_parser.read_file(_f)

    redis_cfg = cfg_parser['redis']
    zerg.common.RedisManager.init_pool(
        ip=redis_cfg.get('ip'),
        port=redis_cfg.getint('port'),
        db=redis_cfg.getint('db'))

    redis_manager = zerg.common.RedisManager(
        stream_name=args.stream_name if args.stream_name else cfg_parser['DEFAULT']['stream_name'])

    ser_cfg_parser = configparser.ConfigParser()
    ser_ini_path = zerg.get_abs_path(args.serial_ini)
    logger.info('Loading config from {}'.format(ser_ini_path))

    with open(ser_ini_path) as _f:
        ser_cfg_parser.read_file(_f)
    ser_cfg = ser_cfg_parser['serial']
    zerg.slave.SerialSlave(redis_manager=redis_manager,
                           client_id=args.client_id,
                           serial_baudrate=ser_cfg.getint('baudrate'),
                           serial_buffer=ser_cfg.getint('buffer'),
                           serial_device=ser_cfg.get('device'),
                           serial_operation_timeout=ser_cfg.getfloat('operation_timeout'),
                           serial_read_terminator=zerg.common.get_terminator_bytes(ser_cfg.get('read_terminator')),
                           serial_read_timeout=ser_cfg.getfloat('read_timeout'),
                           serial_write_timeout=ser_cfg.getfloat('write_timeout')).start()
