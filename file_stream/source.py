import os
import csv
from file_stream.executor import Executor, MysqlExecutor


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

    def __iter__(self, path_root=None):
        for item in self.files:
            yield item


class CsvReader(Executor):
    def __init__(self, path=None, **kwargs):
        """
        从csv读取数据。
        :param dir: 目录地址。
        :param delimiter: 分隔符。
        :param encoding: 文件编码。
        """
        super().__init__()
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
        # TODO 优化来源测试，既能避免未指定来源，又能避免每次迭代都测试，以提高速度。
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
                        yield row
        if self.path is not None:
            with open(self.path, 'r', encoding=self.encoding) as f:
                reader = csv.DictReader(f, delimiter=self.delimiter)
                for row in reader:
                    yield row


class Memory(Executor):
    def __init__(self, items):
        """
        从内存产生数据。
        :param items: 可迭代对象。
        """
        super().__init__()
        self.items = items

    def __iter__(self):
        for item in self.items:
            yield item


class MysqlReader(MysqlExecutor):
    def __init__(self, config: dict, sql: str):
        """
        从mysql读取数据。
        :param config: 数据库配置。
        :param sql: sql语句。
        """
        super().__init__(config)
        self.sql = sql

    def __iter__(self):
        self._connect()

        self.cur.execute(self.sql)
        for item in self.cur:
            yield item

        self._disconnect()


class LineReader(Executor):
    def __init__(self, fpath, **kwargs):
        super().__init__()
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
                        yield row
        if self.path is not None:
            with open(self.path, 'r', encoding=self.encoding) as f:
                for row in f:
                    yield row
