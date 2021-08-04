import sys
import time
import pyspark
#import os
#os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")
from operator import add
from pyspark import SparkContext, SQLContext, StorageLevel, SparkConf
from pyspark.sql import functions
from graphframes import *
from itertools import combinations

start = time.time()

conf = SparkConf() \
    .setAppName('task1') \
    .setMaster('local[2]')
sc = SparkContext.getOrCreate(conf)
sc.setLogLevel("WARN")
sqlContext = SQLContext(sc)

threshold = sys.argv[1]
input_file = sys.argv[2]
output_file = sys.argv[3]

threshold = int(threshold)

#
lines = sc.textFile(input_file).filter(lambda x : x != ('user_id,business_id'))\
            .map(lambda x: x.split(','))
data = lines.map(lambda x: (x[1], x[0])).groupByKey().mapValues(list)\
            .map(lambda x: sorted(x[1]))\
            .map(lambda x: list(combinations(x,2)))\
            .flatMap(lambda x:tuple(sorted(list(x))))\
            .map(lambda x:((x,1))).reduceByKey(add)\
            .filter(lambda x: x[1] >= threshold)\
            .map(lambda x: (x[0][0],x[0][1]))\
            .flatMap(lambda x: ((x[0],x[1]),(x[1],x[0])))
edges = data.collect()

nodes = data.map(lambda x:x[0]).distinct().map(lambda x:(x,x))\
        .collect()

# --- LPA ---
vertices = sqlContext.createDataFrame(nodes,['id', 'user_id'])
edges = sqlContext.createDataFrame(edges, ['src', 'dst'])

g = GraphFrame(vertices, edges)

# --- output ---
res = g.labelPropagation(maxIter=5).groupBy('label')\
            .agg(functions.collect_list('user_id'))\
            .rdd.map(lambda x: x[1]).collect()

for item in res:
    item.sort()
res = sorted(res, key=lambda x: (len(x),x[0]))

with open(output_file, 'w') as f:
    for i in range(len(res)):
        f.write("'" + "', '".join(res[i]) + "'\n")
# output = ''
# for i in ress:
#     for x in i:
#         output = output+'\''+ x+'\',' 
#     output = output[:-2]+'\n'
# with open(output_file,'w') as f:
#     f.write(output)
print("Duration: %s seconds." % (time.time()-start))