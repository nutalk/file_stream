from file_stream.executor.source import Dir, CsvReader, Memory
from file_stream.executor.writer import CsvWriter
from file_stream.executor.writer import MysqlWriter
from db_config import office_base_config


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


def test_mysql():
    datas = [{'f_cuid': 'id2', 'f_sentence_no': 1, 'f_pos_no': 1, 'f_neg_no': 0, 'f_nu_no': 0},
             {'f_cuid': 'id3', 'f_sentence_no': 3, 'f_pos_no': 2, 'f_neg_no': 1, 'f_nu_no': 0},
             {'f_cuid': 'id1', 'f_sentence_no': 1, 'f_pos_no': 1, 'f_neg_no': 0, 'f_nu_no': 0},
             {'f_cuid': 'id4', 'f_sentence_no': 1, 'f_pos_no': 1, 'f_neg_no': 0, 'f_nu_no': 0}, ]
    reader = Memory(datas)
    p = reader | MysqlWriter(office_base_config, 't_report_info')
    p.output()


if __name__ == '__main__':
    test_mysql()
