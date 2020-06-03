# author:hetao
# contact: ht2005112@hotmail.com
# datetime:2020/6/3 下午5:58
# software: PyCharm

"""
文件说明：
"""
from file_stream.executor import Executor


class BroadCaster(Executor):
    def __init__(self, target_list: list, **kwargs):
        super(BroadCaster, self).__init__(**kwargs)
        self._output = target_list

    def __or__(self, executor):
        """or操作，将self放到executor的最左边，同时返回最右边的executor对象。"""
        if not isinstance(executor, Executor):
            raise RuntimeError('下级不是Executor或者子类。')
        source = executor
        while source._source:
            source = source._source
        source._source = self
        self._output.append(source)
        return executor

    def _co(self):
        while True:
            item = yield
            item = self.handle(item)
            for target in self._output:
                target.routine.send(item)
