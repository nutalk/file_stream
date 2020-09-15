from file_stream.executor import Executor, MysqlExecutor
import csv
import copy
from typing import List
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
        self.stream = open(fpath, 'w', newline=kwargs.get('newline', ''))
        self.writer = csv.DictWriter(self.stream, fieldnames=fieldnames, delimiter=kwargs.get('delimiter', ','))
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


class MysqlWriter(MysqlExecutor):
    def __init__(self, config: dict, table_name: str, buffer=100, **kwargs):
        """
        向一个mysql表写入数据。
        :param config: 数据库配置
        :param table_name: 表名称
        :param buffer: 多少条数据commit一次。
        """
        super().__init__(config, **kwargs)
        self.table_name = table_name
        self.buffer = buffer
        self.tmp_item = []

    def _output_many(self, items: list):
        assert isinstance(items, list) and len(items) > 0, '输入只能是list,且长度大于0。'
        assert isinstance(items[0], dict), '元素只能是字典，且字典與数据库列表对应。'
        item_lengs = set([len(item.keys()) for item in items])
        if len(item_lengs) > 1:
            self.logger.warning('注意，输入的字典长度不一致。')
        for item in items:
            field_names = ', '.join(item.keys())
            field_values = ')s, %('.join(item.keys())
            sql = 'replace into {} ({}) values (%({})s)'.format(self.table_name, field_names, field_values)
            self.cur.execute(sql, item)
        self.db.commit()
        self.counter['total'] += len(items)

    def output(self):
        if self._source is None:
            raise IOError('未指定来源')
        self._connect()

        tmp_items = []
        for item in self._source:
            tmp_items.append(copy.deepcopy(item))
            if len(tmp_items) >= self.buffer:
                self._output_many(tmp_items)
                tmp_items = []
        if len(tmp_items) != 0:
            self._output_many(tmp_items)

        self._disconnect()

    def sink(self, item: dict):
        self._output_many([item])

    def writerows(self, rows: List[dict]):
        self._connect()
        self._output_many(rows)
        self._disconnect()

    def output_many(self, rows: List[dict]):
        assert self.db is not None, '请先初始化数据库链接。'
        self._output_many(rows)


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
        self.stream = open(fpath, 'w', newline=kwargs.get('newline', ''))

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


