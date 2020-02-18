#!/usr/bin/env python3
import argparse
import configparser
import logging

import zerg.common
import zerg.master

if __name__ == '__main__':
    logger = logging.getLogger()

    parser = argparse.ArgumentParser("IOC side - Pipeline connection")
    parser.add_argument('--socket-path', type=str)
    parser.add_argument('--stream-name', type=str)
    parser.add_argument('--config-ini', type=str, default='config.ini')
    parser.add_argument('--logging-level', type=str, default='info', choices=['notset', 'debug', 'info', 'warning', 'error', 'critical'])

    args = parser.parse_args()

    zerg.common.log_config(
            level=zerg.common.get_log_level(args.logging_level))

    cfg_parser = configparser.ConfigParser()
    logger.info('Loading config from {}'.format(args.config_ini))
    with open(args.config_ini) as _f:
        cfg_parser.read_file(_f)

    stream_name = args.stream_name if args.stream_name else cfg_parser['DEFAULT'].get('stream_name')
    socket_path = args.socket_path if args.socket_path else cfg_parser['DEFAULT'].get('socket_path')

    redis_cfg = cfg_parser['redis']
    zerg.common.RedisManager.init_pool(
        ip=redis_cfg.get('ip'),
        port=redis_cfg.getint('port'),
        db=redis_cfg.getint('db'))

    redis_manager = zerg.common.RedisManager(
        stream_name=stream_name,
        upstream_timeout=redis_cfg.getint('upstream_timeout'))

    cfg = cfg_parser[zerg.master.STREAMSocketMaster.CFG_STREAM_SOCKET]

    zerg.master.STREAMSocketMaster(redis_manager=redis_manager,
                                   socket_path=socket_path,
                                   socket_read_payload_length=cfg.getboolean('read_payload_length'),
                                   socket_reconnect_interval=cfg.getfloat('reconnect_interval'),
                                   socket_terminator=zerg.common.get_terminator_bytes(cfg.get('terminator')),
                                   socket_timeout=cfg.getfloat('timeout', 5),
                                   socket_use_terminator=cfg.getboolean('use_terminator'),
                                   socket_buffer=cfg.getint('buffer', 1),
                                   # socket_trimm_terminator=cfg.get('trimm_terminator')
                                   ).start()
