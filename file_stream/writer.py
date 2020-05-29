from file_stream.executor import Executor, MysqlExecutor
import csv
import copy
from typing import List, Dict
import redis


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
        if kwargs.get('write_header', True):
            self.writer.writeheader()

    def output(self):
        if self._source is None:
            raise IOError('未指定来源')
        for item in self._source:
            self.counter['total'] += 1
            self.writer.writerow(item)
        self.stream.close()

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

    def _output_many(self, items: list):
        assert isinstance(items, list) and len(items) > 0, '输入只能是list,且长度大于0。'
        assert isinstance(items[0], dict), '元素只能是字典，且字典與数据库列表对应。'
        item_lengs = set([len(item.keys()) for item in items])
        if len(item_lengs) > 1:
            self.logger.warning('注意，输入的字典长度不一致。')
        for item in items:
            field_names = ', '.join(item.keys())
            field_values = ')s, %('.join(item.keys())
            sql = 'insert into {} ({}) values (%({})s)'.format(self.table_name, field_names, field_values)
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

    def writerows(self, rows: List[dict]):
        self._connect()
        self._output_many(rows)
        self._disconnect()

    def output_many(self, rows: List[dict]):
        assert self.db is not None, '请先初始化数据库链接。'
        self._output_many(rows)


class MysqlUpdateWriter(MysqlExecutor):
    def __init__(self, config: dict, table_name: str, primary_keys: list, buffer=100, write_fail=False, **kwargs):
        """
        更新数据库字段。
        :param config: 数据库配置文件
        :param table_name: 表名称
        :param primary_keys: 唯一值
        :param buffer: 缓存多少才commit
        :param write_fail: 更新失败的是否写到数据库里。
        """
        super().__init__(config, **kwargs)
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
        self.counter['write'] += len(items)

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
            self.logger.debug("Number of rows affected by statement '{}': {}".format(result.statement, result.rowcount))
            if result.rowcount == 0:
                miss_update.append(item)
        self.db.commit()
        self.counter['update'] += (len(items) - len(miss_update))

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


class MysqlDel(MysqlWriter):
    def __init__(self, config: dict, table_name: str, key_field: list, buffer=100, **kwargs):
        super(MysqlDel, self).__init__(config, table_name, buffer, **kwargs)
        self.key_field = set(key_field)

    def _output_many(self, items: List[dict]):
        assert isinstance(items, list) and len(items) > 0, '输入只能是list,且长度大于0。'
        assert isinstance(items[0], dict), '元素只能是字典，且字典與数据库列表对应。'
        item_lengs = set([len(item.keys()) for item in items])
        if len(item_lengs) > 1:
            self.logger.warning('注意，输入的字典长度不一致。')
        for item in items:
            element = ['{}=%({})s'.format(item_key, item_key) for item_key in item.keys()]
            field_names = ' and '.join(element)
            sql = 'delete from {} where {}'.format(self.table_name, field_names)
            results = self.cur.execute(sql, item, multi=True)
            result = next(results)
            self.logger.debug("Number of rows affected by statement '{}': {}".format(result.statement, result.rowcount))
            self.counter['total'] += result.rowcount
        self.db.commit()

    def output(self):
        if self._source is None:
            raise IOError('未指定来源')
        self._connect()

        tmp_items = []
        for item in self._source:
            if len(self.key_field - set(item.keys())) > 0:
                raise ValueError('删除对象不对应设定的key field，可能删除多条。')
            item = {key: value for key, value in item.items() if key in self.key_field}
            tmp_items.append(copy.deepcopy(item))
            if len(tmp_items) >= self.buffer:
                self._output_many(tmp_items)
                tmp_items = []
        if len(tmp_items) != 0:
            self._output_many(tmp_items)

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


class RedisWriter(Executor):
    def __init__(self, redis_config: dict, **kwargs):
        super().__init__(**kwargs)
        self.writer = redis.Redis(**redis_config)

    def add_value(self, item: dict):
        for key, value in item.items():
            self.writer.append(key, value)



