#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 25 09:50:41 2018

@author: Mike
"""

from mrjob.job import MRJob

class prob2(MRJob): 
    
    def mapper(self, _, line):
        try:
            cost = line.split(',')[1]
            yield "state", float(cost)
        except:
            pass
  
    def reducer(self, key, value):
        vals = list(value)
        
        mean = sum(vals)/len(vals)
        
        sqd = [(i - mean)**2 for i in vals]
        
        variance = sum(sqd)/len(sqd)
        yield "variance:", variance


if __name__ == '__main__':
    prob2.run()
    
    
"""
CHECK
import numpy as np
price = []
with open("/Users/Mike/Desktop/mapreduce_stats/electricity.csv", 'r') as f:
    for line in f:
        price.append(float(line.split(',')[1]))

variance = np.var(price)  
        
"""