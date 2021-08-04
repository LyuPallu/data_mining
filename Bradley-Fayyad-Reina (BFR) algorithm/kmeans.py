from copy import deepcopy
import enum
from pickle import FALSE
from re import sub
import sys
import time
import json
from pyspark import SparkContext,SparkConf
from collections import defaultdict
import random
import math 
import os
from pathlib import Path


random.seed(50)

start = time.time()
sc = SparkContext(appName="kemans")
sc.setLogLevel('FATAL')

def read_file(file):
    norm_func = lambda x: float(x)/math.pi #* 2 #/math.pi
    points = []
    with open(file, 'r') as f:
        for line in f.readlines():
            line = line.split(',')
            fs = []
            for x in line[1:]:
                fs.append(norm_func(x))
            points.append((line[0], fs))
    return points

def euclidean(x,y):
    dis =0.0
    for i in range(len(x)):
        dis += math.pow((x[1][i] - y[1][i]), 2)
    return math.sqrt(dis)

def find_cluster(point, centroid):
    ds = []
    for i, c in enumerate(centroid):
        if point != c:
            eud = euclidean(point, (0,c))
            ds.append((eud,i))
    min_ds = min(ds)
    return (min_ds[1], point)

def find_centroid(points):
    points = list(points)
    dim = len(points[0][1])
    centroid = []
    for _ in range(dim):
        centroid.append(0)
    for point in points:
        for i in range(dim):
            centroid[i] += point[1][i]
    tmp = []
    for x in centroid:
        tmp.append(x/len(points))
    centroid = tmp
    return centroid

def comp(new_centroid, l_ctd, centroid):
    new_n = l_ctd - len(new_centroid)
    if new_n != 0:
        new_centroid.extend(centroid[:new_n])

        #centroid = deepcopy(sorted(new_centroid))
    new_centroid = sorted(new_centroid)
    return deepcopy(new_centroid)

def kmeans(points, k):
    print('kmena: ', len(points))

    #--init centroid---
    
    ran_id = random.randint(0, len(points))
    #sub_id = int(len(points)/2)
    #start_p = points[ran_id]
    start_p = points[5]
    centroid = [start_p[1]]
    #centroid.append(start_p[1])
    print("start centroid",centroid)
    for _ in range(k-1):
        
        dis = []
        for point in points:
            #calculate
            tmp_ds = []
            for i, c in enumerate(centroid):
                if point != c:
                    # print("point",point)
                    # print("c",c)
                    tmp_ds.append((euclidean(point, (0,c)), i))
            min_ds = min(tmp_ds)
            dis.append((min_ds[0], point))

        max_c = max(dis, key = lambda x:x[0])

        centroid.append(max_c[1][1])

    #--
    time_ = 0
    point_sp = sc.parallelize(points).cache()
    # print('--------', centroid)
    tmp = None
    while time_ < 120 and centroid != tmp:
        time_ += 1
        tmp = centroid
        print('kmeans,find clusters')

        # tmp_clusters = point_sp.map(lambda x: (find_cluster(x, centroid)))
        # clu_list = tmp_clusters.collect()

        clu_list = map(lambda x: (find_cluster(x, centroid)), points)
        clusters = defaultdict(list)
        for k, v in clu_list:
            clusters[k].append(v)

        nc = {}
        for k, v in clusters.items():
            res = find_centroid(v)
            nc[k] = res
        new_centroid = list(nc.values())


        # print('++++++++++++++++++++=', new_centroid)
        # clusters = sc.parallelize(clusters)\
        #                     .groupByKey()
        # clusters = sp_points.map(lambda x: (find_near_c(x, centroids)))
        # new_cents = clusters.groupByKey().mapValues(cal_centroid).map(lambda x: x[1])


        # nc = sc.parallelize(clusters.items()).mapValues(find_centroid).map(lambda x: x[1])
        # print(nc.count())
        # new_centroid = nc.collect()
        # nc.unpersist()
        

        # cur_conv = cal_conv(centroid, new_centroid) 

        centroid = comp(new_centroid, len(centroid), centroid)

        
        
    # res = clusters.mapValues(list).collect()
    # res_clusters = dict(res)
    res_clusters = clusters
    return res_clusters

def point_addition(a, b, pow_):
    res = []
    for i in range(len(a)):
        tmp = a[i] + math.pow(b[i],pow_)
        res.append(tmp)
    return res
def cal_cluster_squ(cluster,DIM):
    _sum = []
    _sumq = []
    for _ in range(DIM):
        _sum.append(0)
        _sumq.append(0)
    for point in cluster:
        _sum = point_addition(_sum, point[1],1)
        _sumq = point_addition(_sumq, point[1],2)
    lv = len(cluster)
    res = [lv, _sum, _sumq]
    return res
    
if __name__ == '__main__':
    input_path = sys.argv[1]
    n_clusters = int(sys.argv[2])
    output_file1 = sys.argv[3]
    
    DS = []
    DS_CLUSTER = []
    SAMPLE_SIZE = 40
   
    points = read_file(input_path)
    #ranom center
    DIM = len(points[0][1]) - 1

    clusters = kmeans(points,n_clusters)
    #print(len(clusters))
    for cluster in clusters.values():
        DS.append(cal_cluster_squ(cluster, DIM))
        idx = []
        for point in cluster:
            idx.append(point[0])
        DS_CLUSTER.append(idx)
    
    output = {}
    for i, v in enumerate(DS_CLUSTER):
        for dx in v:
            output[dx] = i

    print(output)
    with open(output_file1, 'w') as f:
        json.dump(output,f)