import re
import math
from typing import List
import hashlib


DATETIME_FMT = '%Y-%m-%d %H:%M:%S'
DATE_FMT = '%Y-%m-%d'


def split_sentence(sentence) -> list:
    """
    将段落分成句子。
    :param sentence: 需要分句的段落
    :return: list， 句子组成的list
    """
    pattern = r"[。！!？?；;～…◆★]+"
    split_clauses = re.split(pattern, sentence)
    punctuations = re.findall(pattern, sentence)
    punctuations.append('')
    half_out = [''.join(x) for x in zip(split_clauses, punctuations)]
    output = []
    m = r'//'
    for item in half_out:
        split_item = re.split(m, item)
        for item_2 in split_item:
            if item_2 == '':
                continue
            output.append(item_2)
    return output


def split_list(target_list: list, num_elements: int = 5) -> List[list]:
    """
    将list转换成
    :param target_list: 需要拆分的list
    :param num_elements: 每个子列表的数量
    :return: list of list
    """
    return [target_list[i * num_elements: (i + 1) * num_elements] for i in
            range(math.ceil(len(target_list) / num_elements))]


def get_md5_value(src: str) -> str:
    """
    产生md5值。
    :param src: 输入字符
    :return: str， 输出md5字符串
    """
    myd5 = hashlib.md5()
    myd5.update(src.encode("utf8"))
    myd5digest = myd5.hexdigest()
    return myd5digest
