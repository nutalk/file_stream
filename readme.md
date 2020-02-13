# 流式处理数据

## 原理说明

编程主要要到了生成器，各个类用for循环从上游抽取数据，用yield给下游提供数据。通过改写or规则，将各个组建组合起来。

## 参考项目

整体思路主要参考了这个项目：[python-stream](https://github.com/sandabuliu/python-stream)。

## 使用
> pip install file-stream
```python
from file-stream.executor.source import CsvReader
from file-stream.executor.writer import CsvWriter

```
