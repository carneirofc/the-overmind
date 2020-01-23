#!/usr/bin/env python3
import argparse
import configparser
import logging
import common

logger = logging.getLogger()


class BaseSlave:
    """
    Base slave object for synchronous communication.
    """
    def __init__(self, redis_manager: common.RedisManager, client_id: str):
        self.client_id = client_id
        self.client_id_encoded = self.client_id.encode('utf-8')

        self.redis_manager = redis_manager

    def start(self):
        self.redis_manager.slave_upstream_listen(self.do_something)

    def do_something(self, downstream_data):
        logger.warning("Override method {} from {}".format(self.do_something.__name__, self.__str__()))

        return self.client_id_encoded + b"#" + downstream_data + b'\n'


if __name__ == '__main__':
    cfg_parser = configparser.ConfigParser()
    with open('config.ini') as _f:
        cfg_parser.read_file(_f)

    parser = argparse.ArgumentParser("Client side - Pipeline connection")
    parser.add_argument('--stream-name', type=str)
    parser.add_argument('--client-id', type=str)

    common.log_config()

    args = parser.parse_args()
    if not args.client_id:
        client_id = 'cli_1'
    else:
        client_id = args.client_id

    params = {
        'stream_name': args.stream_name if args.stream_name else cfg_parser['DEFAULT']['stream_name'],
    }

    redis_cfg = cfg_parser['redis']
    common.RedisManager.init_pool(
        ip=redis_cfg.get('ip'),
        port=redis_cfg.getint('port'),
        db=redis_cfg.getint('db'))

    redis_manager = common.RedisManager(
        stream_name=args.stream_name if args.stream_name else cfg_parser['DEFAULT']['stream_name'])
    common.log_config()

    BaseSlave(redis_manager=redis_manager, client_id=client_id).start()
