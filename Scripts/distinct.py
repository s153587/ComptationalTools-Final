from mrjob.job import MRJob
from _bins import agebin,timebin

class MRJobDistinct(MRJob):
    
    # Map: keyval = ((age,time),1)
    def mapper(self, _, line):
        answers = line.split(",")
        yield (agebin(answers[0]), timebin(answers[1])), 1
    
    # Red: keyval = ((age,time),sum([1,1..1]))
    def reducer(self, group, counts):
        yield group, sum(counts)
    
if __name__ == '__main__':
    MRJobDistinct.run()