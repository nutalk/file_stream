=============
流式处理数据
=============

利用生成器、协程等流式处理数据。

对数据按多个函数的组合进行过滤、去重，对数据列进行转换，对数据进行计算。

对数据进行质量控制（QC）检查，确保最后的数据与预期相符。

将数据写入csv等。

原理和特点说明
====================

编程主要要到了生成器，各个类用for循环从上游抽取数据，用yield给下游提供数据。通过改写or规则，利用|操作符将各个组建组合起来。

同时支持协程，构建广播、路由等数据节点。

特点：
    - 高拓展性。
    - 低内存占用。
    - 近依赖python基础包。


参考项目
============

整体思路主要参考了这个项目：https://github.com/sandabuliu/python-stream。

安装
========
>>> pip install file-stream


使用
========

从CSV文件读取数据，按条件筛选后输出到屏幕。

::

    reader = CsvReader('/home/hetao/Data/p5w/数据分析/IPO_RoadShow.txt', delimiter='\t', encoding='gbk')
    fit = Filter(lambda x: True)
    writer = ScreenOutput(end='\r')
    p = reader | fit | writer
    p.output()

更多范例参见test文件夹。

