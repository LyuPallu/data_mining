import os
os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

import json
import pandas as pd
import numpy as np
import xgboost as xgb
from surprise import SVD
from surprise import Reader, Dataset
from surprise.dump import dump
import math
import time
import random
from pyspark import SparkContext
from time import strptime, mktime
from collections import defaultdict
from pyspark import SparkContext

random.seed(28)
start = time.time()
sc = SparkContext(appName="train")
svd_n = 3 #cf
avvg = 3.7961611526341503 #cf
scale_n = 5
#train_review = 'data/train_review.json'
#user_file = 'data/user.json'
#business_file = 'data/business.json'
#uavg_path = 'data/user_avg.json'
#bavg_path = 'data/business_avg.json'
train_review = '../resource/asnlib/publicdata/train_review.json'
user_file = '../resource/asnlib/publicdata/user.json'
business_file = '../resource/asnlib/publicdata/business.json'
uavg_path = '../resource/asnlib/publicdata/user_avg.json'
bavg_path = '../resource/asnlib/publicdata/business_avg.json'


print("train ...... ")
lines = sc.textFile(train_review).map(lambda x: json.loads(x))\
            .map(lambda x: ((x['user_id'], x['business_id']), x['stars'])) 

def avgstars(x):
    return ((x[0][0],x[0][1],sum(x[1])/len(x[1])))
lines = lines.groupByKey().map(lambda x:avgstars(x)).collect()
#print(lines)

lines = np.array(lines)
df = pd.DataFrame(lines) 

reader= Reader(name = None,
                line_format=('user item rating'),
                rating_scale=(1,scale_n),
                skip_lines=0)

print('svd')
suprise_data = Dataset.load_from_df(df, reader=reader)
all_trainset = suprise_data.build_full_trainset()

all_testset = all_trainset.build_testset()
test_set = {}
for test in all_testset:
    key = (test[0],test[1])
    value = test[2]
    test_set[key] = value
#print(test_set)

#building SVD
#global SVD_res
SVD_res = []
#redundancy = []
all_training_set = all_trainset.build_testset()
# = svd.test()
#redundancy = [all_trainset]
#parameter 
#redundancy = [all_trainset][-1]
#whole_preds = {}
wh_preds = defaultdict(list)
for i in range(svd_n):
    print("%d svd"%i)
    redun_set = [all_trainset][-1]
    #build SVD
    tmp_svd = SVD(n_factors=1, n_epochs=22)
    tmp_svd.fit(redun_set)
    
    whole_preds = tmp_svd.test(all_training_set)
    for pred in whole_preds:
        uid = pred[0]
        bid = pred[1]
        value = pred[3]
        
        #wh_preds[key] = []
        key = (uid,bid)
        wh_preds[key].append(value)

    SVD_res.append(tmp_svd)

print(SVD_res)

for i in range(svd_n):
    for svd in SVD_res:
        dump('xgb%d.model'%i,None,svd)

user_data = {}

#"yelping_since": "2010-07-05 17:03:04", "useful": 336, 
# "funny": 131, "cool": 160, "elite": "", "friends":[], 
# "fans": 8, "compliment_hot": 3, "compliment_more": 1, "compliment_profile": 0, 
# "compliment_cute": 0, "compliment_list": 0, "compliment_note": 10, "compliment_plain": 15, 
# "compliment_cool": 15, "compliment_funny": 15, "compliment_writer": 4, "compliment_photos": 4\
ftname = [ "useful",  "funny","cool",  "friends",\
               "compliment_hot", "compliment_more", "compliment_profile",\
                   "compliment_cute","compliment_list","compliment_note","yelping_since","compliment_plain",\
                       "compliment_cool","compliment_funny"]
# user_lines = sc.textFile(user_file).map(lambda x: json.loads(x)).collect()
# user_lines = dict(user_lines)
# for items in user_lines.items():
#     print(items)

user_data = defaultdict(list)

#user = rjson(user_file)
def rjson(inputpath):
    with open(inputpath, 'r') as f:
        local_df = json.load(f)
    return local_df
with open(user_file, 'r') as f:
    #lines = f.readline(50)
    lines = f.readlines() #whole file
    for line in lines:
        #print(line)
        udata = json.loads(line)
        key = udata['user_id']
        values = []
        for title in ftname:
            if title == "yelping_since":
                values.append(mktime(strptime(udata["yelping_since"], '%Y-%m-%d %H:%M:%S')))
            elif title == "friends":
                n_fri = len(udata["friends"])
                values.append(n_fri)
            else: 
                values.append(udata[title])
        user_data[key] = values
        
with open('new_user.json', 'w') as f:
    json.dump(user_data, f)

#print(user_data)
#--

user_avg = rjson(uavg_path)
business_avg = rjson(bavg_path)
#business = rjson(business_file)
#wh_preds
new_features = []
for key, values in wh_preds.items():
    uid = key[0]
    bid = key[1]
    
    #read from user_avg
    tmpu = user_avg.get(uid,avvg)
    tmpb = business_avg.get(bid, avvg)
    tmp_feature = [uid, bid,
                   tmpu,tmpb,
                   *values[:3], *user_data.get(uid)]
    new_features.append(tmp_feature)
#print(new_features)

#test_set = {}
row = []
for each in new_features:
    uid = each[0]
    bid = each[1]
    tmp = test_set.get((uid,bid))
    row.append(tmp)
#print(row)
print(len(row))
#print(new_id)

col = pd.DataFrame(new_features)

#print(col) #[990695 rows x 18 columns]'subsample':1,#cf

DM = xgb.DMatrix(data = col.iloc[:,2:].values, label = row)
mymodel = xgb.train(params =  {"objective":"reg:linear",
                     'colsample_bytree': 0.3, #cf
                      'learning_rate': 0.04, #cf
                       'max_depth': 10, #cf
                         'alpha': 11}, #cf
                    dtrain = DM, num_boost_round = 94) #cf    

mymodel.save_model('matrix.model')


print("Duration: %s seconds." % (time.time()-start))   