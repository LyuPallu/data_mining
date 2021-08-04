import sys
import time
import json
from pyspark import SparkContext
import random
import math
import binascii

random.seed(28)

start = time.time()

sc = SparkContext(appName="task1")
sc.setLogLevel('FATAL')

_, first_json_path, second_json_path, output_file = sys.argv

#first_json_path = 'business_first.json'
#second_json_path = 'business_second.json'
#output_file = 'out1'

f = lambda x: x
ch2int = lambda x: int(binascii.hexlify(x.encode('utf8')),16)

lines = sc.textFile(first_json_path).map(lambda x: json.loads(x))\
            .map(lambda x: x['name'])

#l = lines.distinct()
# -- hash -- 
#print(lines.count()) #94296
n = 94296
k = 5
mu = (n*k) / (math.log(2))
parameters = []
for i in range(0,k):
    a = random.randint(1,pow(2,20))
    b = random.randint(1,pow(2,20))
    p = random.randint(1,pow(2,20))
    parameters.append((a,b,p))

#hfs = [lambda x: (((x*a + b))% p) % mu) for (a,b,p) in parameters]

def gen_hfs(x):
    hfs = [((x*a + b) % mu) for (a,b,p) in parameters ] 
    return hfs


train = lines.distinct()
l2int = train.map(ch2int)
l2hash = l2int.flatMap(gen_hfs)

l2hash = l2hash.collect()

#test
lines2 = sc.textFile(second_json_path).map(lambda x: json.loads(x))\
            .map(lambda x: x['name'])
te2int = lines2.map(ch2int)
te2hash = te2int.map(gen_hfs)

l2hash = set(l2hash)
res = te2hash.map(lambda x:set(x))\
                .map(lambda x:'T' if x.issubset(l2hash) else 'F' )

# print(res.take(100))

with open(output_file, 'w') as f:
    f.write(' '.join(res.collect()))
    
print("Duration: % second." % (time.time() - start))

