from mrjob.job import MRJob
from _bins import agebin

class MRJobAgeGroups(MRJob):
    
    # Map: keyval = (age,1)
    def mapper(self, _, line):
        answers = line.split(",")
        yield agebin(answers[0]), 1
    
    # Red: keyval = (age,sum([1,1..1]))
    def reducer(self, agegroup, counts):
        yield agegroup, sum(counts)
    
if __name__ == '__main__':
    MRJobAgeGroups.run()