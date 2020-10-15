import logging
from collections import defaultdict


class Executor(object):
    """
    基础类，定义来源和输出
    """
    def __init__(self, name=None, **kwargs):
        self.name = name or ('%s-%s' % (self.__class__.__name__, id(self)))
        self._source = None
        self._output = None
        self.counter = defaultdict(int)
        if kwargs.get('logger') is not None:
            self.logger = kwargs.get('logger')
        else:
            self.logger = logging.getLogger(__name__)
        self.length = kwargs.get('length', 100)
        self.kwargs = kwargs
        self.routine = self._co()
        next(self.routine)

    def _co(self):
        while True:
            item = yield
            item = self.handle(item)
            if item is None:
                continue
            if self._output is not None:
                self._output.routine.send(item)
            else:
                self.sink(item)

    def sink(self, item):
        """
        在没有下游协程的情况下，处理数据
        :param item:
        :return:
        """
        raise NotImplementedError('需要先实现')

    def run(self):
        """
        启动协程，向下游推送数据。
        :return:
        """
        for item in self:
            self._output.routine.send(item)

    def __iter__(self):
        for item in self._source:
            result = self.handle(item)
            if result is not None:
                yield result

    def handle(self, item):
        """
        对输出进行处理，返回值或者None。
        :param item:
        :return:
        """
        self.counter['total'] += 1
        return item

    def __or__(self, executor):
        """or操作，将self放到executor的最左边，同时返回最右边的executor对象。"""
        if not isinstance(executor, Executor):
            raise RuntimeError('下级不是Executor或者子类。')
        source = executor
        while source._source:
            source = source._source
        source._source = self
        self._output = source
        return executor

    @property
    def source(self):
        if not self._source:
            raise Exception('Lack of data source!')
        return self._source

    def __len__(self):
        if self._source:
            return len(self._source)
        else:
            return self.length

