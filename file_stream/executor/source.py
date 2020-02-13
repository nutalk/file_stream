import os
import csv
from file_stream.executor.executor import Executor


class Dir(Executor):
    def __init__(self, dir: str, allowed_suffix: list):
        """
        获取目录下的所有文件的绝对地址。
        :param dir: 目录地址。
        :param allowed_suffix: 允许的后缀，不包括点。
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
    def __init__(self, path=None):
        """
        csv文件阅读
        :param path: csv文件地址。
        """
        super().__init__()
        self.path = path

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
                with open(fpath, 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        yield row
        if self.path is not None:
            with open(self.path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    yield row
