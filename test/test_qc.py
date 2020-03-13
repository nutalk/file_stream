# author:hetao
# contact: ht2005112@hotmail.com
# datetime:2020/3/13 下午1:56
# software: PyCharm

"""
文件说明：
    测试qc功能
"""

import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)

from file_stream.filter import DataQC, Inspector, NoneFiller
from file_stream.source import Memory
from file_stream.writer import ScreenOutput

import logging


def test_qc():
    datas = [{'f_name': 'tom', 'age': 15, 'height': 167},
             {'f_name': 'tim', 'age': 13},
             {'f_name': 'jim', 'age': '13'},
             {'f_name': 'pim', 'height': ''}, ]
    reader = Memory(datas)

    def null_teller(allow=True):

        def teller(data):
            if allow:
                return True
            elif not allow:
                if data == '' or data is None:
                    return False
                return True

        return teller

    def be_int(data):
        if data == '' or data is None:
            return True
        if isinstance(data, int):
            return True
        return False

    inspector = {'f_name': Inspector(null_teller(True)),
                 'age': Inspector(null_teller(True), be_int, corrective_action=lambda x: int(x))
                 }

    p = reader | NoneFiller(['fname', 'age', 'height']) | DataQC(inspector) | ScreenOutput()
    p.output()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    test_qc()
