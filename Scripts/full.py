from mrjob.job import MRJob
from mrjob.step import MRStep
from _bins import agebin,timebin

class MRJobFull(MRJob):
    
    # Mapper reading input file into binned (age,time) pairs for counting
    # Map: keyval = ((age,time),1)
    def mapper_grouped_answers(self, _, line):
        answers = line.split(",")
        yield (agebin(answers[0]), timebin(answers[1])), 1
    
    # Reducer counting co-occurance of each distinct (age,time) pair
    # Red: keyval = (time_k, (age_i, sum([1,1, ... 1])), i = 0..5, k = 0..5 
    def reducer_grouped_answers_sum(self, agetime, counts):
        age,time = agetime
        yield time, (age, sum(counts))
    
    # Reducer extracting pairs of age pairs and time values
    # Red: keyval = ((agei,agej),(time_k_i],[time_k_j])), 
    #               i = 0..5, j = 1..4 > i, k = 0..5
    def reducer_agegroup_pairs(self, time, agetimevalues):
        # Sort by age group (scales to amount of agebins)
        agetimevalues = sorted(agetimevalues) 
        for agei,timei in agetimevalues:
            for agej,timej in agetimevalues[agei+1:]:
                yield (agei,agej),(timei,timej)
    
    # Mapper mapping all partial-sums for each age-pair
    # Map: keyval = ((group-pair,"var"), var_i),
    #               var = {"n","x","xx","xy","y","y"}, i = 0..n-1
    def mapper_pearson(self, group, timevalues):
        xi,yi = timevalues
        yield (group, "n"),  1
        yield (group, "x"),  xi
        yield (group, "xx"), xi**2
        yield (group, "xy"), xi*yi
        yield (group, "y"),  yi
        yield (group, "yy"), yi**2
    
    # Reducer summarising each group-pair PCC-variables
    # Red: keyval = (group-pair,("var",sum(var_i))),
    #               var = {"n","x","xx","xy","y","y"}, i = 0..n-1
    def reducer_pearson_sum(self, groupvar, partialsums):
        group,var = groupvar
        yield group, (var, sum(partialsums))
    
    # Reudcer extracting variables for each group and computing PCC
    # Red: keyval = (group-pair, PCC(n,x,xx,xy,y,yy))
    def reducer_pearson_compute(self, group, varsumpairs):
        # Sort by var name and extract into variables (6 vars)
        n,x,xx,xy,y,yy = (varsum for _,varsum in sorted(varsumpairs))
        yield group, ( xy-x*y/n ) / ( (xx-(x**2)/n) * (yy-(y**2)/n) )**0.5
    
    # 4 Steps to complete PCC for all group-pairs
    def steps(self):
        return [
            MRStep(mapper=self.mapper_grouped_answers,
                   reducer=self.reducer_grouped_answers_sum),
            MRStep(reducer=self.reducer_agegroup_pairs),
            MRStep(mapper=self.mapper_pearson,
                   reducer=self.reducer_pearson_sum),
            MRStep(reducer=self.reducer_pearson_compute)
        ]
    
if __name__ == '__main__':
    MRJobFull.run()