class Executor(object):
    """
    基础类，定义来源和输出
    """
    def __init__(self, name=None, **kwargs):
        self.name = name or ('%s-%s' % (self.__class__.__name__, id(self)))
        self._source = None
        self._output = None
        self.kwargs = kwargs

    def __iter__(self):
        for item in self._source:
            result = self.handle(item)
            if result is not None:
                yield result

    def handle(self, item):
        """
        对蜀山进行处理，其他类要重写这个方法。
        :param item:
        :return:
        """
        return item

    def __or__(self, executor):
        """or操作，将self放到executor的最左边"""
        source = executor             # type: Executor
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



