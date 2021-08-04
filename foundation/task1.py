import sys
import json
import re
from operator import add
from pyspark import SparkContext

sc=SparkContext(appName="task1")

input_file = sys.argv[1]
output_file = sys.argv[2]
stopwords = sys.argv[3]
y = sys.argv[4]
m = sys.argv[5]
n = sys.argv[6]
n = int(n)

line = sc.textFile(input_file).map(lambda x: json.loads(x))
stopword = sc.textFile(stopwords).flatMap(lambda x: x.split())

#A: total lines
num_reviews = line.count()

#B: total lines in y
num_reviews_y = line.filter(lambda year: y in year['date']).count()

#C: total distinct businesses
num_dis_business = line.map(lambda x: x['business_id']).distinct().count()

#D: top m users -> largest number of review, count
review_users = line.map(lambda x: (x['user_id'],1))
count_review_users = review_users.reduceByKey(add).sortBy(lambda x: (x[1]*(-1),x[0]),True)
top_m_user = count_review_users.take(int(m))

#E top n frequent words in 'text'
review_texts = line.map(lambda x: x['text']).map(lambda x: x.lower())\
                .map(lambda x: x.replace('\n',' '))\
                .map(lambda x: re.sub(r'[^\w\s]','',x))\
                .flatMap(lambda x: x.split(' '))
top_n_review = review_texts.subtract(stopword).map(lambda x: (x,1)).reduceByKey(add)\
                .sortBy(lambda x: (-x[1],x[0]),True)\
                .filter(lambda x: '' not in x).map(lambda x: x[0]).take(n)

output = {"A":num_reviews, "B":num_reviews_y, "C":num_dis_business, "D":top_m_user, "E":top_n_review}

with open(output_file,'w') as f:
    json.dump(output,f)