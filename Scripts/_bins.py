# This file serves as common binning functions for all MapReduce classes

# Bounds found from clusters
_agebounds = [12, 27, 38, 51, 64] # + [ >= 64 ]
_timebounds = [8, 21, 37, 52, 68] # + [ >= 68 ]

# Bound which are better for PCC
_timebounds = [1, 3, 6, 10, 16] # + [ >= 16 ]

def agebin(age):
    age = int(age)
    for i,val in enumerate(_agebounds):
        if age < val:
            return i
    return len(_agebounds)
        
def timebin(time):
    time = int(time)
    for i,val in enumerate(_timebounds):
        if time < val:
            return i
    return len(_timebounds)

def getUpperBounds():
    return _agebounds, _timebounds