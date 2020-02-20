from file_stream.executor import Executor
from file_stream.logic import ExprFunc, And, Or


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
