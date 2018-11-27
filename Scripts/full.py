from mrjob.job import MRJob
from mrjob.step import MRStep
from _bins import agebin,timebin

class MRJobFull(MRJob):
    
    keys = ["n","x","xx","xy","y","yy"]
    
    # Mapper reading input file into binned (age,time) pairs for counting
    # Map: keyval = ((age,time),1)
    def mapper_1(self, _, line):
        answers = line.split(',')
        yield (agebin(answers[0]), timebin(answers[1])), 1
    
    # Reducer counting co-occurance of each distinct (age,time) pair
    # Red: keyval = (age, (time_i, sum([1,1, ... 1])) - i = 0..5
    def reducer_1(self, group, values):
        yield group[0], (group[1], sum(values))
    
    # Reducer collecting list of co-occurancens of age and time-values for each bin sorted by timebin index
    # Red: keyval = (age, [sum(t0), sum(t1), sum(t2), ... sum(t6)])
    def reducer_2(self, a, tvals):
        yield None, (a, [val for _,val in sorted(tvals, key=lambda tval: tval[0])])
    
    # Reducer collecting list of co-occurances for each group-pair
    # Red: keyval = ((groupi,groupj),([vals_i],[vals_j])) - i = 0..5 - j = [1..4] > i
    def reducer_3(self, _, avalpairs):
        avalpairs = list(avalpairs)
        for i,keyvali in enumerate(avalpairs):
            for keyvalj in avalpairs[i+1:]:
                x, vali = keyvali
                y, valj = keyvalj
                key = str(x) + str(y)
                yield key, (vali, valj)
    
    # Mapper mapping the PCC variables for each group-pair
    # Map: keyval = ((group-pair,"var_i"),var_i_j) - var_i = {"n","x","xx","xy","y","y"} - j = 0..n-1
    def mapper_2(self, key, pair):
        x = list(pair[0])
        y = list(pair[1])
        sumx = sum(x)
        sumy = sum(y)
        
        for xi,yi in zip(x,y):
            xi /= sumx # turn into percentages
            yi /= sumy # turn into percentages
            yield (key,self.keys[0]), 1
            yield (key,self.keys[1]), xi
            yield (key,self.keys[2]), xi**2
            yield (key,self.keys[3]), xi*yi
            yield (key,self.keys[4]), yi
            yield (key,self.keys[5]), yi**2
    
    # Reducer summarising each group-pair PCC-variables
    # Red: keyval = (group-pair,("var_i",sum(var_i_j)))  - var_i = {"n","x","xx","xy","y","y"} - j = 0..n-1
    def reducer_4(self, groups_vars, values):
        yield groups_vars[0], (groups_vars[1], sum(values))
    
    # Reudcer extracting variables for each group and computing PCC
    # Red: keyval = (("PCC", group-pair), PCC(n,x,xx,xy,y,yy))
    def reducer_5(self, groups, varsumpairs):
        varsumpairs = dict(varsumpairs)
        n,x,xx,xy,y,yy = [varsumpairs[k] for k in self.keys]
        key = ('PCC',f'Group {groups[0]}', f'Group {groups[1]}')
        yield key, ( xy-x*y/n ) / ( (xx-(x**2)/n) * (yy-(y**2)/n) )**0.5

    # 5 Steps to complete PCC for all group-pairs
    def steps(self):
        return [MRStep(mapper=self.mapper_1, reducer=self.reducer_1),
                MRStep(reducer=self.reducer_2),
                MRStep(reducer=self.reducer_3),
                MRStep(mapper=self.mapper_2, reducer=self.reducer_4),
                MRStep(reducer=self.reducer_5)]
    
if __name__ == '__main__':
    MRJobFull.run()
