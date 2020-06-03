# author:hetao
# contact: ht2005112@hotmail.com
# datetime:2020/6/3 下午3:41
# software: PyCharm

"""
文件说明：
"""
import sys, os

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)

from file_stream import Executor, FieldTrans
from file_stream.source import Memory
from file_stream.writer import ScreenOutput


def test_coroutine():
    datas = [{'f_name': 'tom', 'age': 12},
             {'f_name': 'tim', 'age': 34},
             {'f_name': 'jim'},
             {'f_name': 'pim'}]
    reader = Memory(datas)
    p = reader | FieldTrans({'f_name': 'my_name'}, keep_miss_key=True) | ScreenOutput()
    reader.run()
    # p.output()


if __name__ == '__main__':
    test_coroutine()