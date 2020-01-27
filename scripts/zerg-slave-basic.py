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
    parser.add_argument('--config-ini', type=str, default='config.ini')
    parser.add_argument('--logging-level', type=str, default='info', choices=['notset', 'debug', 'info', 'warning', 'error', 'critical'])

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

    zerg.slave.BaseSlave(redis_manager=redis_manager, client_id=args.client_id).start()
