from datetime import datetime
import sys
import time
import json
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
import random
import math
from math import floor
import binascii

random.seed(28)

_, port_n, output_file = sys.argv
port_n = int(port_n)

sc = SparkContext(appName='fm')
sc.setLogLevel("OFF")

ssc = StreamingContext(sc, 5)
lines = ssc.socketTextStream("localhost", port_n)\
    .window(30,10)\
        .map(lambda x: json.loads(x))\
            .map(lambda x:x['state'])

#hash
k = 100
mu = 2**10
ch2int = lambda x: int(binascii.hexlify(x.encode('utf8')), 16)
int2bin = lambda x: format(x, '032b')
bin2tail = lambda x: len(str(x)) - len(str(x).rstrip("0"))
def cal_zer0(x):
    x = int2bin(x)
    x = bin2tail(x)
    return x #int

p = []
for i in range(0,k):
    a = random.randint(1,pow(2,20))
    b = random.randint(1,pow(2,20))
    p.append((a,b))
    
def fm(data):
    #time
    local_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
    
    #grd
    d = data.collect()
    grd = len(set(d))
    #print(grd)

    #est
    res = []
    #lines = data.collect()
    for i in range(k):
        #print("first for")
        l2tail = 0
        for lin in d:
            #print("second for")
            lin = ch2int(lin)
            a = p[i][0]
            b = p[i][1]
            l2hash = (a*lin + b)%mu
            
            tmp = (cal_zer0(l2hash) if l2hash != 0 else 0)
            
            if tmp >= l2tail:
                l2tail = tmp
                #print(l2tail)
        res.append(2**l2tail)
    print(res)
    '''
    res = sorted(res)
    sublen = floor(len(res)//2-1)
    esti = res[sublen]
    
    '''

    sublen = floor(len(res)//3)
    r1 = sum(res[:sublen]) / sublen
    r2 = sum(res[sublen: 2*sublen]) / sublen #median
    r3 = sum(res[2*sublen:]) / sublen
    esti = sorted([r1,r2,r3])[1]
    print([r1,r2,r3])
    print(esti)
    print(grd)
    esti = int(esti)
    
    if abs(grd - esti)/esti <= 0.25:
        with open(output_file, 'a') as f:
            f.write(
                str(local_time) + "," + str(grd) + "," + str(esti) +"\n")
                #

with open (output_file, 'w') as f:
    f.write('Time,Ground Truth,Estimation' + '\n')
    f.close()

rdd = lines.foreachRDD(lambda x: fm(x))

ssc.start()            
ssc.awaitTermination()
#print("Duration: % second." % (time.time() - start))