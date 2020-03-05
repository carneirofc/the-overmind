#!/usr/bin/env python3
import argparse
import logging

import zerg.common
import zerg.slave

if __name__ == '__main__':
    logger = logging.getLogger()

    parser = argparse.ArgumentParser("Client side")
    parser.add_argument('--logging-level', type=str, default='info',
                        choices=['notset', 'debug', 'info', 'warning', 'error', 'critical'])
    parser.a

    args = parser.parse_args()

    zerg.common.log_config(level=zerg.common.get_log_level(args.logging_level))

    redis_config = zerg.common.get_application_config('the-overmind')
    beagle_config = zerg.common.get_beagle_config()
    app_config = zerg.common.get_application_config(beagle_config.app)

    redis_manager = zerg.common.RedisManager(
        ip=redis_config['ip'], port=redis_config['port'], db=redis_config['db'],
        stream_name=beagle_config.endpoint)

    zerg.slave.SerialSlave(redis_manager=redis_manager,
                           client_id=beagle_config.ip,
                           priority=beagle_config.priority,
                           serial_baudrate=app_config['serial']['baudrate'],
                           serial_buffer=app_config['serial']['buffer'],
                           serial_device=app_config['serial']['device'],
                           serial_operation_timeout=app_config['serial']['operation_timeout'],
                           serial_read_terminator=zerg.common.get_terminator_bytes(app_config['serial']['terminator']),
                           serial_read_timeout=app_config['serial']['read_timeout'],
                           serial_use_terminator=app_config['serial']['use_terminator'],
                           serial_write_timeout=app_config['serial']['write_timeout'],
                           ).start()

