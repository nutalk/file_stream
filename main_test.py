from file_stream.executor.source import Dir, CsvReader, Memory, MysqlReader
from file_stream.executor.writer import CsvWriter
from file_stream.executor.writer import MysqlWriter


office_base_config = {
    'host': "",
    'user': "",
    'passwd': '',
    'database': '',
    'charset': 'utf8',
}


def test_csv():
    """
    从文件读取，从目录读取所有文件迭代输出。
    :return:
    """
    reader = CsvReader('/home/hetao/Data/p5w/数据分析/IPO_RoadShow.txt', delimiter='\t', encoding='gbk')
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


def test_mysql_write():
    datas = [{'f_name': 'tom'},
             {'f_name': 'tim'},
             {'f_name': 'jim'},
             {'f_name': 'pim'}, ]
    reader = Memory(datas)
    p = reader | MysqlWriter(office_base_config, 't_table')
    p.output()


def test_mysql_read():
    sql = 'SELECT * FROM test.t_table;'
    reader = MysqlReader(office_base_config, sql)
    for row in reader:
        print(row)


if __name__ == '__main__':
    # test_mysql_read()
    test_csv()