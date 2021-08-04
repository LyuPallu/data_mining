import sys
from pyspark import SparkContext
import json
import re

review_file = sys.argv[1]
business_file = sys.argv[2]
output_file = sys.argv[3]
if_spark = sys.argv[4]
n = sys.argv[5]
n = int(n)

def cal_average(list):
    sum = 0
    for tmp in list:
        sum += tmp
    return sum/len(list)


if if_spark == 'spark':
    sc=SparkContext(appName="task2")
    review_data = sc.textFile(review_file).map(lambda x: json.loads(x))\
                .map(lambda x : (x['business_id'],x['stars']))
    business_data = sc.textFile(business_file).map(lambda x: json.loads(x))\
                .map(lambda x : (x['business_id'], str(x['categories'])\
                .replace("\n"," ").split(',')))
    join_data=business_data.join(review_data).map(lambda x:[x[1][1],x[1][0]]).flatMapValues(lambda x:x)\
                .map(lambda x : (x[1].strip(),x[0]))\
                .aggregateByKey((0,0), lambda U,v: (U[0] + v,  U[1] + 1), \
                               lambda U1,U2: (U1[0] + U2[0], U1[1] + U2[1]))\
                .map(lambda x: (x[0],float(x[1][0])/x[1][1]))                                          #average
    final=join_data.sortBy(lambda x:(x[1]*(-1),x[0]),True).take(n)
    output={"result":final}

elif if_spark == 'no_spark':
    #review
    reviews = [json.loads(line) for line in open(review_file,'r')]
    re_dict={}
    for review in reviews:
        re_dict.setdefault(review['business_id'],[]).append(review['stars'])
     
    #average
    for re in re_dict:
        re_dict[re] = cal_average(re_dict[re])

    #business
    #with open(business_file,'r') as f:
     #   for line in f:
      #      businesses = json.loads(line)
    businesses= [json.loads(line) for line in open(business_file, 'r',  encoding='UTF-8')]
    business_dict = {}
    for business in businesses:
        business_dict.setdefault(business['business_id'], []).append(
            [i.strip() for i in str(business['categories']).split(',')])
        for v in business['business_id'][0]:
            v.strip()
    
    #mix
    star_ = {}
    for tmp in business_dict:
        if tmp in re_dict:
            for cate_ in business_dict[tmp][0]:
                star_.setdefault(cate_,[]).append(re_dict[tmp])

    result_ = []
    for s in star_:
        tmp = (s,cal_average(star_[s]))
        result_.append(tmp)
    result_tmp = sorted(result_, key=lambda x: (-x[1],x[0]))
    result_output = result_tmp[:n]
    output={"result":result_output}

else:
    output['error'] = 'wrong if_spark parm'

with open(output_file,'w') as f:
    json.dump(output,f)