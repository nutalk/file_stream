from file_stream.executor import Executor
from file_stream.logic import ExprFunc, And, Or, FullOut
from collections import defaultdict
from typing import Callable
from inspect import isfunction
import logging


class Filter(Executor):
    def __init__(self, *args, **kwargs):
        """
        过滤器。
        :param mix:判断器的组合方法，可以用'and', 'or'。
        :param args: 判断器或者函数组成的列表。
        """
        mix = kwargs.get('mix', 'and')
        assert mix in ['and', 'or'], "mix 参数只能是 'and', 'or'。"
        super().__init__()
        if not args:
            self.filter = ExprFunc(lambda x: True)
        if mix == 'and':
            self.filter = And(*args)
        elif mix == 'or':
            self.filter = Or(*args)

    def handle(self, item):
        if self.filter.result(item):
            self.counter['pass'] += 1
            return item
        else:
            self.counter['block'] += 1
            return None


class FinishedRemove(Executor):
    def __init__(self, target: Executor, target_fields: list, source_fields: list = None, trans_str=True, stop_till: int=0, **kwargs):
        """
        去除已经完成的内热.
        :param target: Executor, 迭代器即可.最终的输出目标,从这个目标读取已经完成的内容.
        :param target_fields: 最终输出列,将这些列组成tuple,用于判断是否完成.
        :param source_fields: 来源列,与最终输出进行对比的列,用于判断是否已经完成.若未指定则认为与target_fields相同.
        :param trans_str: 是否统一转换成str。
        :param stop_till: 多少重复的之后停止。
        """

        super().__init__(**kwargs)
        self.exists = set()
        self.target = target
        self.target_fields = target_fields
        self.trans_str = trans_str
        self.stop_till = stop_till
        self.finished = 0
        if source_fields is not None:
            self.source_fields = source_fields
        else:
            self.source_fields = target_fields
        self._get_finished()

    def _get_finished(self):
        for row in self.target:
            if self.trans_str:
                handler = tuple([str(row[field]) for field in self.target_fields])
            else:
                handler = tuple([row[field] for field in self.target_fields])
            self.exists.add(handler)

    def __iter__(self):
        for item in self._source:
            result = self.handle(item)
            if self.stop_till != 0 and self.finished >= self.stop_till:
                self.logger.info('遇到{}次已完成的记录，代码退出。'.format(self.finished))
                break
            if result is not None:
                yield result

    def handle(self, item):
        if self.trans_str:
            handler = tuple([str(item[field]) for field in self.source_fields])
        else:
            handler = tuple([item[field] for field in self.source_fields])

        if handler in self.exists:
            self.logger.debug('pass data, {}.'.format(item))
            self.finished += 1
            self.counter['finished'] += 1
            return None
        else:
            self.logger.debug('dealing with, {}.'.format(item))
            self.counter['new'] += 1
            return item


class DuplicateRemove(Executor):
    def __init__(self, tell_fields: list, **kwargs):
        """
        删除重复内容。
        :param tell_fields: 用来对比是否重复的列名称组成的列表。

          remover = DuplicateRemove(['f_company_code', 'f_question'])
        """
        super().__init__(**kwargs)
        self.tell_fields = tell_fields
        self.exists = set()

    def handle(self, item):
        handler = tuple([item[field].replace('\n', '') for field in self.tell_fields])
        if handler in self.exists:
            self.logger.debug('find duplicate data, {}.'.format(item))
            self.counter['duplicate'] += 1
            return None
        else:
            self.counter['new'] += 1
            return item

    def __iter__(self):
        for item in self._source:
            result = self.handle(item)
            if result is not None:
                handler = tuple([item[field] for field in self.tell_fields])
                self.exists.add(handler)
                yield result


class FieldTrans(Executor):
    def __init__(self, trans_dict: dict, keep_miss_key: bool = False):
        """
        列名称转换。
        :param trans_dict: 列名称转换字典，key为来源处的列名称，value为转换后输出的列名称。
        :param keep_miss_key: 是否包括trans_dict中未包含的字段.
        """
        super().__init__()
        self.trans_dict = trans_dict
        self.keep_miss_key = keep_miss_key

    def handle(self, item):
        self.counter['total'] += 1
        out_row = {}
        if self.keep_miss_key:
            out_row.update(item)
        for source_key, output_key in self.trans_dict.items():
            out_row[output_key] = item[source_key]
            if self.keep_miss_key:
                out_row.pop(source_key)
        return out_row


def default_action(item):
    raise ValueError('遇到错误数据. {}'.format(item))


def inspect_null(item):
    for key, value in item.items():
        if value == '' or value is None:
            logging.debug('data qc: 遇到空值: {}: {}.'.format(key, value))
            return False
    return True


class DataQC(Executor):
    def __init__(self, *args, corrective_action: Callable = None, **kwargs):
        """
        数据质量检查.
        :param args: 判断函数.
        :param corrective_action: 遇到错误的修复措施.
        :param kwargs: mix: [and | or]
        """
        super().__init__()
        mix = kwargs.get('mix', 'and')
        assert mix in ['and', 'or'], "mix 参数只能是 'and', 'or'。"
        self.mix = mix

        if corrective_action is not None:
            assert isfunction(corrective_action), '请输入一个函数.'
            self.corrective_action = corrective_action
        else:
            self.corrective_action = default_action

        self.correct_record_no = defaultdict(int)
        self.total_record_no = 0
        self.filter = FullOut(*args)

    def handle(self, item):
        result = self.filter.result(item)
        for idx, tell in enumerate(result):
            if tell:
                self.correct_record_no[idx] += 1
        self.total_record_no += 1

        if self.mix == 'and':
            single_result = all(result)
        else:
            single_result = any(result)

        if single_result:
            self.counter['pass'] += 1
            return item
        else:
            self.counter['fix'] += 1
            return self.corrective_action(item)

    @property
    def report(self):
        return {'total_items': self.total_record_no, 'correct_record': self.correct_record_no}


class NoneFiller(Executor):
    def __init__(self, fieldnames, fill_with=None, **kwargs):
        super().__init__(**kwargs)
        self.fieldnames = fieldnames
        self.fill_with = fill_with

    def handle(self, item):
        for key in self.fieldnames:
            if item.get(key) is None:
                item[key] = self.fill_with
        return item
