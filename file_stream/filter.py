from file_stream.executor import Executor
from file_stream.logic import ExprFunc, And, Or
from collections import UserDict
import logging
from typing import Dict, Callable
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
    def __init__(self, trans_dict: dict):
        """
        列名称转换。
        :param trans_dict: 列名称转换字典，key为来源处的列名称，value为转换后输出的列名称。
        """
        super().__init__()
        self.trans_dict = trans_dict

    def handle(self, item):
        out_row = {}
        for source_key, output_key in self.trans_dict.items():
            out_row[output_key] = item[source_key]
        return out_row


class Inspector(UserDict):
    def __init__(self, *funcs, mix='and', corrective_action: Callable = None):
        """
        数据检查
        :param funcs: 数据点检查函数列表
        :param mix: 函数组合方式
        :param corrective_action: 修正函数
        """
        assert mix in ['and', 'or'], "mix 参数只能是 'and', 'or'。"
        if corrective_action is not None:
            assert isfunction(corrective_action), 'corrective_action 应该是一个函数。'

        super().__init__()
        self['functions'] = funcs
        self['mix'] = mix
        self['corrective_action'] = corrective_action


def default_correction(*args, **kwargs):
    logging.debug('trying to correct the data.')
    raise ValueError('DataQC quality control fail')


class DataQC(Executor):
    def __init__(self, inspect_dict: Dict[str, Inspector], default_correction_function=None):
        """
        数据质量控制，按照规则对指定列的数据进行检查，并执行纠正措施。
        :param inspect_dict: {field_name：Inspector}组成的字典。
        """
        super().__init__()
        self.inspect_dict = {}  # 列名称：检查方式
        self.corrective_action = {}  # 纠正措施

        # 设置纠错方式
        if default_correction_function is None:
            self.default_correction_function = default_correction
        else:
            self.default_correction_function = default_correction_function

        # 设置检查方式
        for key, inspect in inspect_dict.items():
            if inspect.get('mix', 'and') == 'and':
                self.inspect_dict[key] = And(*inspect['functions'])
            else:
                self.inspect_dict[key] = Or(*inspect['functions'])
            if inspect.get('corrective_action') is None:
                self.corrective_action[key] = self.default_correction_function
            else:
                self.corrective_action[key] = inspect.get('corrective_action')

    def handle(self, item):
        for key, expr in self.inspect_dict.items():
            if expr.result(item[key]):
                logging.debug('qc pass, key: {}, value: {}'.format(key, item[key]))
                # 如果通过检查
                continue
            else:
                # 如未通过检查
                logging.debug('qc fail, key: {}, value: {}, value_type {}'.format(key, item[key], type(item[key])))
                item[key] = self.corrective_action[key](item[key])
        return item


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
