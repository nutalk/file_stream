# author:hetao
# contact: ht2005112@hotmail.com
# datetime:2020/5/29 下午4:53
# software: PyCharm

"""
文件说明：
"""

import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from pprint import pprint


from file_stream.source import KafkaReader
from file_stream.writer import RedisWriter
from confg import kafka_config, redis_config
import logging


TOPIC = os.environ.get('TOPIC', 'demo.crawled_interaction_crawler')


def kafka_test():
    reader = KafkaReader(kafka_config, TOPIC)
    for item in reader:
        pprint(item)
        _ = input()


def redis_test():
    r = RedisWriter(redis_config)
    cninfo_platrorm_name = 'cninfo'
    print(cninfo_platrorm_name)
    out = r.writer.get('interaction_{}_reply_ids'.format(cninfo_platrorm_name))
    pprint(out)


if __name__ == '__main__':
    logging.basicConfig(format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
                        datefmt='%Y-%m-%d %H:%M:%S', level=logging.DEBUG)
    # kafka_test()
    redis_test()
