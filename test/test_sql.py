# author:hetao
# contact: ht2005112@hotmail.com
# datetime:2020/3/11 上午9:58
# software: PyCharm

"""
文件说明：
测试sql写入
"""

import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)

from file_stream.writer import MysqlWriter, MysqlUpdateWriter, CsvWriter, MysqlDel
from file_stream.source import Memory, CsvReader, Dir, MysqlReader
from file_stream.executor import Executor

import logging
from confg import office_base_config, yun_base_config, office_main_config


class GoogleTrendTrans(Executor):
    """
    将下载的csv文件写到数据库。
    """
    def __iter__(self):
        for fpath in self.source:
            if 'HK' in fpath:
                area = '香港'
            else:
                area = '澳门'

            reader = CsvReader(fpath)
            for row in reader:
                for key, value in row.items():
                    outrow = {'f_date': row['date'], 'f_area': area}
                    if key == 'date' or key == 'isPartial':
                        continue
                    else:
                        outrow['f_keyword'] = key
                        outrow['f_index'] = value
                        logging.debug('sending row {}'.format(outrow))
                        yield outrow


def test_csv_mysql():
    p = Dir('/home/hetao/Data/yiqing/bsvi/csv/', ['csv']) | GoogleTrendTrans() | MysqlWriter(office_base_config, 'bsvi_google')
    p.output()


def test_csv_update_mysql():
    data = [{'f_keyword': '曹彬', 'f_date': '2019-10-01', 'f_area': '澳门', 'f_index': '11'}]
    reader = Memory(data)
    p = reader | MysqlUpdateWriter(office_base_config, 'bsvi_google', ['f_keyword', 'f_date', 'f_area'])
    p.output()


def test_mysql_csv():
    yun_base_config['database'] = 'datago_shenzhen'
    reader = MysqlReader(yun_base_config, "select qn_id_plat, platform, stock_id, qn_time from investor_question_info where qn_time < '2020-01-01';")
    files = {}
    for platform in ['sseinfo', 'cninfo', 'panorama']:
        files[platform] = CsvWriter('/home/hetao/Data/interaction/output/{}.csv'.format(platform),
                                    ['qn_id_plat', 'platform', 'stock_id', 'qn_time'])
    for row in reader:
        files[row['platform']].writerow(row)


def test_mysql_delete():
    deler = MysqlDel(office_main_config, 'test_table', ['f_name'])
    datas = [{'f_name': 'tom'},
             {'f_name': 'tim'},
             {'f_name': 'jim'},
             {'f_name': 'pim'}]
    pipe = Memory(datas) | deler
    pipe.output()


if __name__ == '__main__':
    logging.basicConfig(format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
                        datefmt='%Y-%m-%d %H:%M:%S', level=logging.DEBUG)
    test_mysql_delete()
