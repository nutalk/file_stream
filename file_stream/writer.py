from file_stream.executor import Executor, MysqlExecutor
import csv
import logging
import copy
from typing import List


class CsvWriter(Executor):
    def __init__(self, fpath: str, fieldnames: list, **kwargs):
        """
        写csv文件。
        :param fpath: 目标地址。
        :param fieldnames: 表头组成。
        :param delimiter: 分隔符。
        """
        super().__init__()
        self.stream = open(fpath, 'w')
        self.writer = csv.DictWriter(self.stream, fieldnames=fieldnames, delimiter=kwargs.get('delimiter', ','))
        self.writer.writeheader()

    def output(self):
        if self._source is None:
            raise IOError('未指定来源')
        for item in self._source:
            self.writer.writerow(item)
        self.stream.close()

    def writerow(self, row: dict):
        assert isinstance(row, dict), '输入必须是字典。'
        self.writer.writerow(row)


class MysqlWriter(MysqlExecutor):
    def __init__(self, config: dict, table_name: str, buffer=100):
        """
        向一个mysql表写入数据。
        :param config: 数据库配置
        :param table_name: 表名称
        :param buffer: 多少条数据commit一次。
        """
        super().__init__(config)
        self.table_name = table_name
        self.buffer = buffer

    def _output_many(self, items: list):
        # TODO 检查不同行的数据key的数量是否一致，不一致应该给出提醒。logging.warning.
        assert isinstance(items, list) and len(items) > 0, '输入只能是list,且长度大于0。'
        assert isinstance(items[0], dict), '元素只能是字典，且字典與数据库列表对应。'
        field_names = ', '.join(items[0].keys())
        field_values = ')s, %('.join(items[0].keys())
        sql = 'insert into {} ({}) values (%({})s)'.format(self.table_name, field_names, field_values)
        for item in items:
            self.cur.execute(sql, item)
        self.db.commit()
        logging.debug('sql commit')

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

    def writerows(self, rows: List[dict]):
        self._connect()
        self._output_many(rows)
        self._disconnect()


class MysqlUpdateWriter(MysqlExecutor):
    def __init__(self, config: dict, table_name: str, primary_keys: list, buffer=100, write_fail=False):
        """
        更新数据库字段。
        :param config: 数据库配置文件
        :param table_name: 表名称
        :param primary_keys: 唯一值
        :param buffer: 缓存多少才commit
        :param write_fail: 更新失败的是否写到数据库里。
        """
        super().__init__(config)
        self.table_name = table_name
        self.buffer = buffer
        self.primary_keys = primary_keys
        self.write_fail = write_fail

    def _output_many(self, items: list):
        assert isinstance(items, list) and len(items) > 0, '输入只能是list,且长度大于0。'
        assert isinstance(items[0], dict), '元素只能是字典，且字典與数据库列表对应。'
        field_names = ', '.join(items[0].keys())
        field_values = ')s, %('.join(items[0].keys())
        sql = 'insert into {} ({}) values (%({})s)'.format(self.table_name, field_names, field_values)
        for item in items:
            self.cur.execute(sql, item)
        self.db.commit()

    def _update_many(self, items: list):
        assert isinstance(items, list) and len(items) > 0, '输入只能是list,且长度大于0。'
        assert isinstance(items[0], dict), '元素只能是字典，且字典與数据库列表对应。'
        assert set(self.primary_keys).issubset(items[0].keys()), '{}中未包括所有主键{}'.format(items[0].keys(), self.primary_keys)
        set_str = ' , '.join(['{} = %({})s'.format(inf, inf) for inf in items[0].keys() - set(self.primary_keys)])
        where_str = ' AND '.join(['{} = %({})s'.format(inf, inf) for inf in self.primary_keys])
        sql = 'UPDATE {} SET {}  WHERE {}'.format(self.table_name, set_str, where_str)
        miss_update = []
        for item in items:
            results = self.cur.execute(sql, item, multi=True)
            result = next(results)
            logging.debug("Number of rows affected by statement '{}': {}".format(result.statement, result.rowcount))
            if result.rowcount == 0:
                miss_update.append(item)
        self.db.commit()
        if self.write_fail and len(miss_update) > 0:
            self._output_many(miss_update)

    def output(self):
        if self._source is None:
            raise IOError('未指定来源')
        self._connect()

        tmp_items = []
        for item in self._source:
            tmp_items.append(copy.deepcopy(item))
            if len(tmp_items) >= self.buffer:
                self._update_many(tmp_items)
                tmp_items = []
        if len(tmp_items) != 0:
            self._update_many(tmp_items)

        self._disconnect()


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

    def writerow(self, row: dict):
        print(row, end=self.end)
