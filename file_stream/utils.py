import re


def split_sentence(sentence):
    """
    将段落分成句子。
    :param sentence:
    :return: list
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

