# author:hetao
# contact: ht2005112@hotmail.com
# datetime:2020/4/9 下午2:13
# software: PyCharm

"""
文件说明：
    统计目录内的文件名特定规则下的数量。
"""

from file_stream.source import Dir
from file_stream.executor import Executor
from file_stream.writer import CsvWriter
from collections import defaultdict


class SurfixSt(Executor):
    def __init__(self):
        super(SurfixSt, self).__init__()
        self.surfix = defaultdict(int)

    def __iter__(self):
        for fpath in self.source:
            _, _, _, stc, *_ = fpath.split('_')
            self.surfix[stc] += 1
        for key, count in self.surfix.items():
            yield {'stock_id': key, 'cnt': count}


def stat_surfix(indir, outf):
    p = Dir(indir) | SurfixSt() | CsvWriter(outf, ['stock_id', 'cnt'])
    p.output()


if __name__ == '__main__':
    stat_surfix('/media/hetao/my_data/tmp/SZ_whole/', '/media/hetao/my_data/tmp/stock_id.csv')
