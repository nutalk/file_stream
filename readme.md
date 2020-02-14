# 流式处理数据

从目录读取所有文件，从csv读取所有数据，对数据计算后，写入csv、数据库等。

## 原理说明

主要要到了生成器，各个类用for循环从上游抽取数据，用yield给下游提供数据。通过改写or规则，将各个组建组合起来。

## 参考项目

整体思路主要参考了这个项目：https://github.com/sandabuliu/python-stream。

## 安装

> pip install file-stream

## 使用

### 获取目录下所有文件名称。
```python
from file_stream.executor.source import Dir

fdir = Dir('/home/hetao/Data/p5w/tmp2', ['csv'])
for fpath in fdir:
    print(fpath)
```

### 写数据到数据库。

```python
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

```
