# encoding=UTF-8
'''
Created on 2013-7-18
@author: Administrator
'''
from timeit import itertools
from random import random, Random
import datetime

src = list(itertools.product(range(0, 10), repeat=5))
random = Random()
tdelta = datetime.timedelta(seconds=1)

def gen(filename, start, end, filters):
    
    with open(filename, 'w') as f:
        dt = start
        while dt < end:
            f.write(dt.strftime("%Y%m%d%H%M%S") + ":" + u",".join(map(unicode, random.choice(src))) + "\n")
            dt = dt + tdelta
            if filters is not None and dt < end and filters(dt):
                print 'next ' + dt.strftime("%Y-%m-%d %H:%M:%S")

    print 'finish write to ' + filename + "from " + start.strftime("%Y-%m-%d %H:%M:%S") + " to " + end.strftime("%Y-%m-%d %H:%M:%S")

if __name__ == '__main__':

    gen(r"D:\test\hadoop_src\2013",
        datetime.datetime(2013, 01, 01, 0, 0, 0),
        datetime.datetime(2013, 02, 01, 0, 0, 0),
        lambda dt:dt.hour == 0 and dt.minute == 0 and dt.second == 0)
    
