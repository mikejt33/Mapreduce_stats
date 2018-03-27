#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 25 09:23:01 2018

@author: Mike
"""
from mrjob.job import MRJob

class prob1(MRJob): 
    
    def mapper(self, _, line):
        try:
            population = int(line.split(',')[4])
            state = line.split(',')[0]
            yield "states", population
        except:
            pass
        
    def reducer(self, key, value):
        
        pops = list(value)
        
        yield "min", min(pops)
        yield "max", max(pops)
        yield "average", (sum(pops)/len(pops))

if __name__ == '__main__':
    prob1.run()