=============
流式处理数据
=============

原理说明
=============

编程主要要到了生成器，各个类用for循环从上游抽取数据，用yield给下游提供数据。通过改写or规则，将各个组建组合起来。

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

    from file_stream.executor.source import Memory
    from file_stream.executor.writer import MysqlWriter

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
