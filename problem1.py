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
            area = int(line.split(',')[3])
            yield "states", (population,area)
        except:
            pass

    def reducer(self, key, value):
        
        val = list(value)
        
        pop =[i[0] for i in val]
        area = [i[1] for i in val]

        yield "min population", min(pop)
        yield "max population", max(pop)
        yield "average population", sum(pop)/len(pop)
        yield "min area", min(area)
        yield "max area", max(area)
        yield "average area", sum(area)/len(area)


if __name__ == '__main__':
    prob1.run()