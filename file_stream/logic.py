from inspect import isfunction


class ExprFunc(object):
    def __init__(self, func):
        """
        判断器，判断函数的封装
        :param func: 用来判断的函数。
        """
        assert isfunction(func), '需要传递函数'
        self.operator = func

    def result(self, data):
        """
        接受一条数据，根据函数判断是否符合。
        """
        result = self.operator(data)
        assert isinstance(result, bool), '函数返回值非bool，请修改函数。'
        return result


class ExprMix(object):
    def __init__(self, name: str,  *args):
        """
        将判断器进行组装。
        :param name: 名称，由于字符串打印实例信息。
        :param args: 判断器或者函数组成的列表。
        """
        self.name = name
        self.args = []
        for arg in args:
            if isinstance(arg, ExprFunc):
                self.args.append(arg)
            else:
                self.args.append(ExprFunc(arg))

    def result(self, data):
        raise NotImplementedError('请在子类中实现。')

    def __str__(self):
        return self.name.join(['(%s)' % i for i in self.args])


class And(ExprMix):
    def __init__(self, *args):
        super().__init__(' And', *args)

    def result(self, data):
        return all([v.result(data) for v in self.args])


class Or(ExprMix):
    def __init__(self, *args):
        super().__init__(' Or', *args)

    def result(self, data):
        return any([v.result(data) for v in self.args])


class FullOut(ExprMix):
    """
    输出所有函数的判断结果.
    """
    def __init__(self, *args):
        super().__init__(' full', *args)

    def result(self, data):
        return [v.result(data) for v in self.args]


if __name__ == '__main__':
    func1 = ExprFunc(lambda x: True)
    func2 = ExprFunc(lambda x: False)

    test_and = And(func1, lambda x:False)
    test_or = Or(func1, func2, lambda x:True)

    print(test_and.result(1))
    print(test_or.result(2))


