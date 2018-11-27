# This file serves as common binning functions for all MapReduce classes

_agebins = [19, 25, 35, 50, 64] # + [ >= 64 ]
_timebins = [1, 3, 6, 10, 16] # + [ >= 16 ]

def agebin(age):
    age = int(age)
    for i,val in enumerate(_agebins):
        if age < val:
            return i
    return len(_agebins)
        
def timebin(time):
    time = int(time)
    for i,val in enumerate(_timebins):
        if time < val:
            return i
    return len(_timebins)
