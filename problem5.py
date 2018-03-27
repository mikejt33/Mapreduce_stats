#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 26 10:30:01 2018

@author: Mike
"""

from mrjob.job import MRJob
import random 


class problem5(MRJob):
    def mapper(self,_,line):
        school = line.split(',')[0]
        if school != 'College Name':
            #yield random.sample(school,100)
            yield (random.randint(1,100), school)
                
          
    def reducer(self,key,value):
        v = list(value)
        yield 'school', v[random.randint(0, len(v)-1)]

            
if __name__ == '__main__':
    problem5.run()