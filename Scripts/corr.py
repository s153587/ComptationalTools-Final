import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep
from _bins import agebin,timebin

class MRJobPearsonCorr(MRJob):
    
    # Remove key from output
    OUTPUT_PROTOCOL = mrjob.protocol.JSONValueProtocol
    
    # Mapper mapping each partial sum of each sum-variable for each line in file
    # Map: keyval = ("var_i",var_i_j), var_i = {"n","x","xx","xy","y","y"}
    def mapper(self, _, line):
        answers = line.split(",")
        xi,yi = agebin(answers[0]), timebin(answers[1])
        yield "n",  1,
        yield "x",  xi
        yield "xx", xi*xi
        yield "xy", xi*yi
        yield "y",  yi
        yield "yy", yi*yi
    
    # Reducer summarising each variable
    # Map: keyval = ("var_i",sum(var_i_j)),
    #               var_i = {"n","x","xx","xy","y","y"}, j = 0..n-1
    def reducer_sum(self, var, values):
        yield None, (var, sum(values))
     
    # Reducer extracting variables and computing PCC
    # Map: value = PCC(n,x,xx,xy,y,yy)
    def reducer_pearson(self, _, varsumpairs):
        # Sort by var name and extract into variables (6 vars)
        n,x,xx,xy,y,yy = (varsum for _,varsum in sorted(varsumpairs))
        yield None, ( xy-x*y/n ) / ( (xx-(x**2)/n) * (yy-(y**2)/n) )**0.5
    
    # 2 step to calculate PCC for age and time
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_sum),
            MRStep(reducer=self.reducer_pearson)
        ]
    
if __name__ == '__main__':
    MRJobPearsonCorr.run()
