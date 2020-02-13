from file_stream.executor.source import Dir, CsvReader
from file_stream.executor.writer import CsvWriter


def test_csv():
    """
    从文件读取，从目录读取所有文件迭代输出。
    :return:
    """
    reader = CsvReader('/home/hetao/Data/p5w/output/output_roadshow.csv')
    for row in reader:
        print(row)
        break

    fdir = Dir('/home/hetao/Data/p5w/tmp2', ['csv'])
    p = fdir | CsvReader()
    for row in p:
        print(row)
        break


def test_csv_write():
    """
    从目录读取所有文件，合并到同一个csv输出。
    :return:
    """
    p = Dir('/home/hetao/Data/p5w/tmp2', ['csv']) | CsvReader()
    writer = CsvWriter('/home/hetao/Data/p5w/output/output_question.csv', p.fieldnames)
    p = p | writer
    p.output()


if __name__ == '__main__':
    test_csv_write()
