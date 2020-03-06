#!/usr/bin/env python3
import argparse
import logging

import zerg.common
import zerg.master

if __name__ == '__main__':
    logger = logging.getLogger()

    parser = argparse.ArgumentParser("IOC side - Pipeline connection")
    parser.add_argument('app', type=str, choices=['uhv', 'mks'])
    parser.add_argument('endpoint', type=str)
    parser.add_argument('socket_path', type=str)

    parser.add_argument('--logging-level', type=str, default='info',
                        choices=['notset', 'debug', 'info', 'warning', 'error', 'critical'])

    args = parser.parse_args()

    app = args.app
    endpoint = args.endpoint
    socket_path = args.socket_path

    zerg.common.log_config(level=zerg.common.get_log_level(args.logging_level))

    redis_config = zerg.common.get_application_config('the-overmind')
    master_config = zerg.common.get_master_data(app, endpoint)
    app_config = zerg.common.get_application_config(app)
    stream_config = zerg.common.StreamConfig(app_config['stream'], application=app)

    redis_manager = zerg.common.RedisManager(
        ip=redis_config['ip'], port=redis_config['port'], db=redis_config['db'],
        stream_name=endpoint)

    zerg.master.STREAMSocketMaster(redis_manager=redis_manager,
                                   socket_path=socket_path,
                                   socket_read_payload_length=stream_config.read_payload_length,
                                   socket_reconnect_interval=stream_config.reconnect_interval,
                                   socket_terminator=zerg.common.get_terminator_bytes(stream_config.terminator),
                                   socket_timeout=stream_config.timeout,
                                   socket_buffer=stream_config.buffer,
                                   socket_trim_terminator=stream_config.trim_terminator,
                                   ).start()
