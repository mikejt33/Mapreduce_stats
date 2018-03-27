from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy as np

alpha1 = 0
beta1 = 0
alpha2 = 0
beta2 = 0

class normalequation(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper,
                   reducer=self.reducer1),
                MRStep(reducer=self.reducer2)]
    def mapper(self, _, line):
        try:
            values = line.split(',')
            if len(values) == 2: # electricity (name, price)
                if values[0] != '' and values[1] != '':
                    name = values[0]
                    price = float(values[1])
                    yield name, dict([('price', price)])
            else: # states (name, _, _, area, population)
                if values[0] != '' and values[0] != 'United States' and values[3] != '' and values[4] != '':
                    name = values[0]
                    area = int(values[3])
                    population = int(values[4])
                    yield name, dict([('area', area), ('population', population)])
        except:
            pass
    def reducer1(self, key, values):
        v = list(values)
        if 'price' in v[0]:
            Y = v[0]['price']
            X = (1, v[1]['area'], v[1]['population'])
        else:
            Y = v[1]['price']
            X = (1, v[0]['area'], v[0]['population'])
        yield 'data', (X, Y)
    def reducer2(self, key, values):
        global alpha1, alpha2, beta1, beta2
        v = list(values)
        X, Y = zip(*v)
        X = np.array(X)
        X1 = np.delete(X, 2, 1)
        X2 = np.delete(X, 1, 1)
        Y = np.array(Y)
        Y.shape = (len(Y),1)
        betas1 = np.dot(np.linalg.inv(np.dot(X1.T, X1)), np.dot(X1.T, Y))
        betas2 = np.dot(np.linalg.inv(np.dot(X2.T, X2)), np.dot(X2.T, Y))
        alpha1 = betas1[0][0]
        beta1 = betas1[1][0]
        alpha2 = betas2[0][0]
        beta2 = betas2[1][0]
        yield 'alpha1', alpha1
        yield 'beta1', beta1
        yield 'alpha2', alpha2
        yield 'beta2', beta2

class sse(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper,
                   reducer=self.reducer1),
                MRStep(reducer=self.reducer2)]
    def mapper(self, _, line):
        try:
            values = line.split(',')
            if len(values) == 2: # electricity (name, price)
                if values[0] != '' and values[1] != '':
                    name = values[0]
                    price = float(values[1])
                    yield name, dict([('price', price)])
            else: #states (name, _, _, area, population)
                if values[0] != '' and values[0] != 'United States' and values[3] != '' and values[4] != '':
                    name = values[0]
                    area = int(values[3])
                    population = int(values[4])
                    yield name, dict([('area', area), ('population', population)])
        except:
            pass
    def reducer1(self, key, values):
        global alpha1, alpha2, beta1, beta2
        v = list(values)
        if 'price' in v[0]:
            y = v[0]['price']
            area = v[1]['area']
            population = v[1]['population']
        else:
            y = v[1]['price']
            area = v[0]['area']
            population = v[0]['population']
        yhat1 = area * alpha1 + beta1
        yhat2 = population * alpha2 + beta2
        yield 'SSE1', (yhat1 - y)**2
        yield 'SSE2', (yhat2 - y)**2
    def reducer2(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    normalequation.run()
    sse.run()