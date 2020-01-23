#!/usr/bin/env python3
import argparse
import configparser
import logging
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
        self.redis_manager.slave_upstream_listen(self.do_something)

    def do_something(self, downstream_data):
        logger.warning("Override method {} from {}".format(self.do_something.__name__, self.__str__()))

        return self.client_id_encoded + b"#" + downstream_data + b'\n'


if __name__ == '__main__':

    parser = argparse.ArgumentParser("Client side - Pipeline connection")
    parser.add_argument('--stream-name', type=str)
    parser.add_argument('--client-id', type=str, default='cli_1')
    parser.add_argument('--config-ini', type=str, default='config.ini')

    zerg.common.log_config()

    args = parser.parse_args()

    cfg_parser = configparser.ConfigParser()
    cfg_ini_path = zerg.get_abs_path(args.config_ini)
    logger.info('Loading config from {}'.format(cfg_ini_path))
    with open(cfg_ini_path) as _f:
        cfg_parser.read_file(_f)

    params = {
        'stream_name': args.stream_name if args.stream_name else cfg_parser['DEFAULT']['stream_name'],
    }

    redis_cfg = cfg_parser['redis']
    zerg.common.RedisManager.init_pool(
        ip=redis_cfg.get('ip'),
        port=redis_cfg.getint('port'),
        db=redis_cfg.getint('db'))

    redis_manager = zerg.common.RedisManager(
        stream_name=args.stream_name if args.stream_name else cfg_parser['DEFAULT']['stream_name'])
    zerg.common.log_config()

    BaseSlave(redis_manager=redis_manager, client_id=args.client_id).start()
