from file_stream.executor.executor import Executor
import csv


class CsvWriter(Executor):
    def __init__(self, fpath:str, fieldnames:list):
        super().__init__()
        self.stream = open(fpath, 'w')
        self.writer = csv.DictWriter(self.stream, fieldnames=fieldnames)
        self.writer.writeheader()

    def output(self):
        if self._source is None:
            raise IOError('未指定来源')
        for item in self._source:
            self.writer.writerow(item)
        self.stream.close()