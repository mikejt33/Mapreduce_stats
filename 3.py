#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 25 10:20:01 2018

@author: Mike

        top = (n*sumxy) - (sumx)*(sumy)
        bottomx = (n*xx) - (xx**2)
        bottomy = (n*yy) - (yy**2)
        bottom = bottomx * bottomy
        bottom = math.sqrt(bottom)
        
        alpha = top/bottom
        
        
"""

from mrjob.job import MRJob
import math

class prob3(MRJob): 
    
    def mapper(self, _, line):
        try:
            population = int(line.split(',')[4])
            area = int(line.split(',')[3])
            yield "states", (population,area)
        except:
            pass
  
    def reducer(self, key, value):
        val = list(value)
        
        y =[i[0] for i in val]
        x = [i[1] for i in val]
        xy = [a*b for a,b in zip(x,y)]
        xx = [a*b for a,b in zip(x,x)]
        yy = [a*b for a,b in zip(y,y)]
        sumx = sum(x)
        sumy = sum(y)
        sumxy = sum(xy)
        sumxx = sum(xx)
        sumyy = sum(yy)
        n = len(y)
        

        alphatop = (sumy*sumxx) - (sumx * sumxy)
        alphabottom = (n*sumxx) - (sumx**2)
        
        alpha = alphatop/alphabottom
        
        betatop = (n*sumxy) - (sumx * sumy)
        betabottom = (n*sumxx) - (sumx**2)
        
        beta = betatop/betabottom
        
        yield "alpha:", alpha
        yield "beta:", beta


if __name__ == '__main__':
    prob3.run()
    
###Check
from sklearn.linear_model import LinearRegression
import pandas as pd
p=[]
a=[]
with open("/Users/Mike/Desktop/mapreduce_stats/states.csv", 'r') as f:
    for line in f:
        try:
            p.append(int(line.split(',')[-1]))
            a.append(int(line.split(',')[-2]))
        except:
            pass
a.reshape(-1,1)
p.reshape(-1,1)
reg = LinearRegression()
reg.fit(a,p)
alp = reg.coef_
b = reg.intercept_
print(alp,b)

    