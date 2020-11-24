import os
#os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
#os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'
import sys
import json
import pandas as pd
import numpy as np
import xgboost as xgb
from surprise import SVD
from surprise import Reader, Dataset
from surprise.dump import dump
from surprise.dump import load
import math
import time
import random
from pyspark import SparkContext
from time import strptime, mktime
from collections import defaultdict
from pyspark import SparkContext

random.seed(28)
start = time.time()
sc = SparkContext(appName="test")
svd_n = 3 #cf
ft_n = 14
avvg = 3.7961611526341503 #cf
scale_n = 5

test_file = sys.argv[1]
output_file = sys.argv[2]

#uavg_path = 'data/user_avg.json'
#bavg_path = 'data/business_avg.json'
uavg_path = '../resource/asnlib/publicdata/user_avg.json'
bavg_path = '../resource/asnlib/publicdata/business_avg.json'


#read test data
lines = sc.textFile(test_file).map(lambda x: json.loads(x))\
            .map(lambda x: (x['user_id'], x['business_id']))

def avgstars(x):
    return ((x[0][0],x[0][1],sum(x[1])/len(x[1])))
lines = lines.map(lambda x: (x,1)).groupByKey()\
                .map(lambda x:avgstars(x)).collect()

lines = np.array(lines)
df = pd.DataFrame(lines) 
#print(df)
reader= Reader(name = None,
                line_format=('user item rating'),
                rating_scale=(1,scale_n),
                skip_lines=0)

suprise_data = Dataset.load_from_df(df, reader=reader)
all_testset = suprise_data.build_full_trainset()

#read xgb data
ddd = []

for i in range(svd_n):
    tmp = load('xgb%d.model'%i)
    ddd.append(tmp[1])

#run model
all_testset = all_testset.build_testset()
rough_preds = defaultdict(list)
for d in ddd:
    preds = d.test(all_testset)

    for pred in preds:
        uid = pred[0]
        bid = pred[1]
        key = (uid, bid)
        rough_preds[key].append(pred[3])

def rjson(inputpath):
    with open(inputpath, 'r') as f:
        local_df = json.load(f)
    return local_df

user_avg = rjson(uavg_path)
business_avg = rjson(bavg_path)
new_user = rjson('new_user.json') #col=13
lo = []
for i in range(ft_n):
    lo.append(0)

new_features = []
for key, value in rough_preds.items():
    uid = key[0]
    bid = key[1]
    tmpu = user_avg.get(uid,avvg)
    tmpb = business_avg.get(bid, avvg)
    tmp_feature = [uid, bid,
                   tmpu,tmpb,
                   *value, 
                   *new_user.get(uid,lo)]
    new_features.append(tmp_feature)
    
#build DMatrix 
col = pd.DataFrame(new_features)
#print(col) #[990695 rows x 18 columns]

# xgb build matrix
DM = xgb.DMatrix(data = col.iloc[:,2:].values, label = None)
booster = xgb.Booster()
booster.load_model('matrix.model')
preds = booster.predict(DM)

'''
i = 0
r = []
for ft in new_features:
    cur = {'user_id':ft[0],'business_id':ft[1],'stars': float(preds[i])}
    r.append(cur)
    i += 1
print(r)
'''
#print(res)

with open(output_file,'w') as f:
    i = 0
    for ft in new_features:
        cur = {'user_id':ft[0],'business_id':ft[1],'stars': float(preds[i])}
        f.write(json.dumps(cur))
        f.write('\n')
        i += 1


print("Duration: %s seconds." % (time.time()-start))   