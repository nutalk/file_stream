from file_stream.executor.executor import Executor, MysqlExecutor
import csv


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

    def __output_many(self, items: list):
        assert isinstance(items, list) and len(items)>0, '输入只能是list,且长度大于0。'
        assert isinstance(items[0], dict), '元素只能是字典，且字典與数据库列表对应。'
        field_names = ', '.join(items[0].keys())
        field_values = ')s, %('.join(items[0].keys())
        sql = 'insert into {} ({}) values (%({})s)'.format(self.table_name, field_names, field_values)
        for item in items:
            self.cur.execute(sql, item)
        self.db.commit()

    def output(self):
        if self._source is None:
            raise IOError('未指定来源')
        self._connect()

        tmp_items = []
        for item in self._source:
            tmp_items.append(item)
            if len(tmp_items) >= self.buffer:
                self.__output_many(tmp_items)
                tmp_items = []
        if len(tmp_items) != 0:
            self.__output_many(tmp_items)

        self._disconnect()
