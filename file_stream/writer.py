from file_stream.executor import Executor
import csv
import json


class CsvWriter(Executor):
    def __init__(self, fpath: str, fieldnames: list, **kwargs):
        """
        写csv文件。
        :param fpath: 目标地址。
        :param fieldnames: 表头组成。
        :param delimiter: 分隔符。
        """
        super().__init__()
        self.stream = open(fpath, 'w', newline=kwargs.get('newline', ''), **kwargs)
        self.writer = csv.DictWriter(self.stream, fieldnames=fieldnames, **kwargs)
        if kwargs.get('write_header', True):
            self.writer.writeheader()

    def output(self):
        if self._source is None:
            raise IOError('未指定来源')
        for item in self._source:
            self.counter['total'] += 1
            self.writer.writerow(item)
        self.stream.close()

    def sink(self, item: dict):
        self.writerow(item)

    def writerow(self, row: dict):
        assert isinstance(row, dict), '输入必须是字典。'
        self.writer.writerow(row)
        self.counter['total'] += 1


class ScreenOutput(Executor):
    def __init__(self, end='\n'):
        """在屏幕打印出输出结果。"""
        super().__init__()
        self.end = end

    def output(self):
        if self._source is None:
            raise IOError('未指定来源')

        for item in self._source:
            print(item, end=self.end)

    def sink(self, item):
        self.writerow(item)

    def writerow(self, row: dict):
        print(row, end=self.end)


class JsonWriter(Executor):
    def __init__(self, fpath: str, **kwargs):
        """
        写json到文件。
        :param fpath: 目标地址。
        """
        super().__init__()
        self.stream = open(fpath, 'w', **kwargs)

    def output(self):
        if self._source is None:
            raise IOError('未指定来源')
        for item in self._source:
            item.pop('md_id')
            self.counter['total'] += 1
            self.stream.write(json.dumps(item, ensure_ascii=False))
            self.stream.write('\n')
        self.stream.close()

    def sink(self, item: dict):
        self.writerow(item)

    def writerow(self, row: dict):
        assert isinstance(row, dict), '输入必须是字典。'
        self.stream.write(json.dumps(row, ensure_ascii=False))
        self.stream.write('\n')
        self.counter['total'] += 1


