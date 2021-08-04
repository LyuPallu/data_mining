import sys
from pyspark import SparkContext
import json
import re
from operator import add
from pyspark.rdd import portable_hash

review_file = sys.argv[1]
output_file = sys.argv[2]
partition_type = sys.argv[3]
n_partitions = sys.argv[4]
n_partitions = int(n_partitions)
n = sys.argv[5]
n = int(n)

sc = SparkContext(appName="task3")

if partition_type == 'default':
    review_data = sc.textFile(review_file).map(lambda x: json.loads(x))\
        .map(lambda x: (x['business_id'],1))
    num_part = review_data.getNumPartitions()
    num_item = review_data.glom().map(len).collect()
    _result_ = review_data.reduceByKey(add)\
            .filter(lambda x: x[1] > int(n)).collect()

elif partition_type == 'customized':
    review_data = sc.textFile(review_file).map(lambda x: json.loads(x))\
        .map(lambda x: (x['business_id'],1))\
        .partitionBy(n_partitions,lambda x: hash(x))
    num_part = review_data.getNumPartitions()
    num_item = review_data.glom().map(len).collect()
    _result_ = review_data.reduceByKey(add).filter(lambda x: x[1]>int(n)).collect()

outputs = {"n_partitions":num_part, "n_items":num_item, "result":_result_}
with open(output_file,'w') as f:
    json.dump(outputs,f) 

