
import mysql.connector
from retry import retry
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
        self.kwargs = kwargs

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


class MysqlExecutor(Executor):
    def __init__(self, config: dict, **kwargs):
        """
        向一个mysql表读取或写入数据的基础类。
        :param config: 数据库配置
        """
        super().__init__(**kwargs)
        self.config = config
        self.db = None
        self.cur = None

    @retry(tries=3, delay=1)
    def _connect(self, dictionary=True):
        self.db = mysql.connector.connect(**self.config)
        self.cur = self.db.cursor(dictionary=dictionary)

    def connect(self, dictionary=True):
        self._connect(dictionary)

    @retry(tries=3, delay=1)
    def _disconnect(self):
        self.cur.close()
        self.db.close()

    def disconnect(self):
        self._disconnect()
