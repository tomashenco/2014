
from mrjob.job import MRJob
import time


class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        yield "chars", len(line)
        yield "words", len(line.split())
        yield "lines", 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    t1 = time.time()
    MRWordFrequencyCount.run()
    print "CPU Time", time.time() - t1