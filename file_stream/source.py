import os
import csv
from file_stream.executor import Executor, MysqlExecutor
import json
from confluent_kafka import Consumer


class Dir(Executor):
    def __init__(self, dir: str, allowed_suffix: list = None):
        """
        获取目录下的所有文件的绝对地址。
        :param dir: 目录地址。
        :param allowed_suffix: 允许的后缀，None的情况下返回全部。
        """
        super().__init__()
        self.dir = dir
        self.allowed_suffix = allowed_suffix
        self.files = self.__get_files(dir, allowed_suffix)
        self._source = self.files

    def __get_files(self, root_path, file_suffix: list = None):
        container = []
        dir_or_files = os.listdir(root_path)
        for dir_file in dir_or_files:
            dir_file_path = os.path.join(root_path, dir_file)
            if os.path.isdir(dir_file_path):
                sub_container = self.__get_files(dir_file_path, file_suffix)
                container += sub_container
            else:
                if file_suffix is None:
                    container.append(dir_file_path)
                else:
                    file_name, suffix = os.path.splitext(dir_file_path)
                    if suffix[1:] in file_suffix:
                        container.append(dir_file_path)
                    else:
                        continue
        return container


class CsvReader(Executor):
    def __init__(self, path=None, **kwargs):
        """
        从csv读取数据。
        :param dir: 目录地址。
        :param delimiter: 分隔符。
        :param encoding: 文件编码。
        """
        super().__init__(**kwargs)
        self.path = path
        self.delimiter = kwargs.get('delimiter', ',')
        self.encoding = kwargs.get('encoding', 'utf8')

    def test_source(self):
        if self._source is None and self.path is None:
            raise IOError('未指定读取来源')
        if self._source is not None and self.path is not None:
            raise IOError('来源重复，请检查')

    @property
    def fieldnames(self):
        self.test_source()
        if self._source is not None:
            for fpath in self._source:
                with open(fpath, 'r') as f:
                    reader = csv.DictReader(f)
                    return reader.fieldnames
        if self.path is not None:
            with open(self.path, 'r') as f:
                reader = csv.DictReader(f)
                return reader.fieldnames

    def __iter__(self):
        self.test_source()
        if self._source is not None:
            for fpath in self._source:
                with open(fpath, 'r', encoding=self.encoding) as f:
                    reader = csv.DictReader(f, delimiter=self.delimiter)
                    for row in reader:
                        self.counter['total'] += 1
                        yield row
        if self.path is not None:
            with open(self.path, 'r', encoding=self.encoding) as f:
                reader = csv.DictReader(f, delimiter=self.delimiter)
                for row in reader:
                    self.counter['total'] += 1
                    yield row


class Memory(Executor):
    def __init__(self, items, **kwargs):
        """
        从内存产生数据。
        :param items: 可迭代对象。
        """
        super().__init__(**kwargs)
        self._source = items


class MysqlReader(MysqlExecutor):
    def __init__(self, config: dict, sql: str, **kwargs):
        """
        从mysql读取数据。
        :param config: 数据库配置。
        :param sql: sql语句。
        """
        super().__init__(config, **kwargs)
        self.sql = sql

    def __iter__(self):
        self._connect()

        self.cur.execute(self.sql)
        for item in self.cur:
            self.counter['total'] += 1
            yield item

        self._disconnect()


class LineReader(Executor):
    def __init__(self, fpath, **kwargs):
        super().__init__(**kwargs)
        self.path = fpath
        self.encoding = kwargs.get('encoding', 'utf8')

    def test_source(self):
        if self._source is None and self.path is None:
            raise IOError('未指定读取来源')
        if self._source is not None and self.path is not None:
            raise IOError('来源重复，请检查')

    def __iter__(self):
        self.test_source()
        if self._source is not None:
            for fpath in self._source:
                with open(fpath, 'r', encoding=self.encoding) as f:
                    for row in f:
                        self.counter['total'] += 1
                        yield row
        if self.path is not None:
            with open(self.path, 'r', encoding=self.encoding) as f:
                for row in f:
                    self.counter['total'] += 1
                    yield row


class KafkaReader(Executor):
    def __init__(self, kafka_config, topic, timeout: int = 10, **kwargs):
        super().__init__(**kwargs)
        self.consumer = Consumer(kafka_config)
        self.consumer.subscribe([topic])
        self.timeout = timeout

    def __iter__(self) -> dict:
        while True:
            msg = self.consumer.poll(self.timeout)
            if not msg:
                self.logger.debug('No message from kafka')
                continue
            if msg.error():
                self.logger.exception("Consumer error: {}".format(msg.error()))
                continue
            message = json.loads(msg.value())
            yield message
