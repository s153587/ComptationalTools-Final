from mrjob.job import MRJob
from mrjob.step import MRStep
from _bins import agebin,timebin

class MRJobPearsonCorr(MRJob):
    
    keys = ["n","x","xx","xy","y","yy"]
    
    # Mapper mapping each partial sum of each sum-variable for each line in file
    # Map: keyval = ("var_i",var_i_j) - var_i = {"n","x","xx","xy","y","y"}
    def mapper(self, _, line):
        answers = line.split(",")
        xi,yi = agebin(answers[0]), timebin(answers[1])
        yield self.keys[0], 1,
        yield self.keys[1], xi
        yield self.keys[2], xi*xi
        yield self.keys[3], xi*yi
        yield self.keys[4], yi
        yield self.keys[5], yi*yi
    
    # Reducer summarising each variable
    # Map: keyval = ("var_i",sum(var_i_j)) - var_i = {"n","x","xx","xy","y","y"} - j = 0..n-1
    def reducer_sum(self, var, values):
        yield None, (var, sum(values))
     
    # Reducer extracting variables and computing PCC
    # Map: keyval = ("PCC", PCC(n,x,xx,xy,y,yy))
    def reducer_pearson(self, _, varsumpairs):
        varsumpairs = dict(varsumpairs)
        n,x,xx,xy,y,yy = [varsumpairs[k] for k in self.keys]
        yield "PCC", ( xy-x*y/n ) / ( (xx-(x**2)/n) * (yy-(y**2)/n) )**0.5
    
    # 2 step to calculate PCC for age and time
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_sum),
            MRStep(reducer=self.reducer_pearson)
        ]
    
if __name__ == '__main__':
    MRJobPearsonCorr.run()
