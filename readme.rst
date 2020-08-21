=============
流式处理数据
=============

从目录读取所有文件，从csv读取所有数据，从mysql读取数据。

对数据按多个函数的组合进行过滤、去重， 对数据列进行转换，对数据进行计算。

对数据进行质量控制（QC）检查，确保最后的数据与预期相符。

将数据写入csv、数据库等。

原理和特点说明
====================

编程主要要到了生成器，各个类用for循环从上游抽取数据，用yield给下游提供数据。通过改写or规则，利用|操作符将各个组建组合起来。

同时支持协程，构建广播、路由等数据节点。

特点：
    - 高拓展性。
    - 低内存占用。
    - 丰富的代码注释与样例。


参考项目
============

整体思路主要参考了这个项目：https://github.com/sandabuliu/python-stream。

安装
========
>>> pip install file-stream


使用
========
写数据到数据库。

::

    from file_stream.source import Memory
    from file_stream.writer import MysqlWriter

    office_base_config = {
        'host': "",
        'user': "",
        'passwd': '',
        'database': '',
        'charset': '',
    }

    datas = [{'f_cuid': 'id2', 'f_sentence_no': 1, 'f_pos_no': 1, 'f_neg_no': 0, 'f_nu_no': 0},
             {'f_cuid': 'id3', 'f_sentence_no': 3, 'f_pos_no': 2, 'f_neg_no': 1, 'f_nu_no': 0},
             {'f_cuid': 'id1', 'f_sentence_no': 1, 'f_pos_no': 1, 'f_neg_no': 0, 'f_nu_no': 0},
             {'f_cuid': 'id4', 'f_sentence_no': 1, 'f_pos_no': 1, 'f_neg_no': 0, 'f_nu_no': 0}, ]
    reader = Memory(datas)
    p = reader | MysqlWriter(office_base_config, 't_report_info')
    p.output()

从CSV文件读取数据，按条件筛选后输出到屏幕。

::

    reader = CsvReader('/home/hetao/Data/p5w/数据分析/IPO_RoadShow.txt', delimiter='\t', encoding='gbk')
    fit = Filter(lambda x: True)
    writer = ScreenOutput(end='\r')
    p = reader | fit | writer
    p.output()

更多范例参见test文件夹。

