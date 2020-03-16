from file_stream.executor import Executor
from file_stream.logic import ExprFunc, And, Or, FullOut
from collections import defaultdict
import logging
from typing import Callable
from inspect import isfunction


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
            return item
        else:
            return None


class DuplicateRemove(Executor):
    def __init__(self, tell_fields: list):
        """
        删除重复内容。
        :param tell_fields: 用来对比是否重复的列名称组成的列表。

          remover = DuplicateRemove(['f_company_code', 'f_question'])
        """
        super().__init__()
        self.tell_fields = tell_fields
        self.exists = set()

    def handle(self, item):
        handler = tuple([item[field].replace('\n', '') for field in self.tell_fields])
        if handler in self.exists:
            logging.debug('find duplicate data, {}.'.format(item))
            return None
        else:
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
            return item
        else:
            return self.corrective_action(item)

    @property
    def report(self):
        return {'total_items': self.total_record_no, 'correct_record': self.correct_record_no}


class NoneFiller(Executor):
    def __init__(self, fieldnames, fill_with=None):
        super().__init__()
        self.fieldnames = fieldnames
        self.fill_with = fill_with

    def handle(self, item):
        for key in self.fieldnames:
            if item.get(key) is None:
                item[key] = self.fill_with
        return item
