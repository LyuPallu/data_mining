from copy import deepcopy
from pickle import FALSE
from re import sub
import sys
import time
import json
from pyspark import SparkContext,SparkConf
import random
from pyspark import StorageLevel
from collections import defaultdict
import math
import os
from pathlib import Path
# os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
# os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

random.seed(38)

start = time.time()

conf = SparkConf()
conf.set("spark.executor.cores", "2")
sc = SparkContext(appName="bfr", conf=conf)
sc.setLogLevel('FATAL')


def euclidean(x,y):
    dis =0.0
    for i in range(len(x)):
        dis += math.pow((x[1][i] - y[1][i]), 2)
    return math.sqrt(dis)

def read_file(file):
    points = []
    with open(file, 'r') as f:
        for line in f.readlines():
            line = line.split(',')
            fs = []
            for x in line[1:]:
                fs.append(float(x))
            points.append((line[0], fs))
    return points

def find_cluster(point, centroid):
    ds = []
    # print(point)
    for i, c in enumerate(centroid):
        if point != c:
            # print(c)
            if c == 0:
                print(point, centroid)
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
    

def comp(new_centroid, l_ctd):
    new_n = l_ctd - len(new_centroid)
    if new_n != 0:
        new_centroid.extend(centroid[:new_n])

        #centroid = deepcopy(sorted(new_centroid))
    new_centroid = sorted(new_centroid)
    return deepcopy(new_centroid)
    

def cal_conv(c1, c2):
    dis = 0
    for p1, p2 in zip(c1, c2):
        for i in range(len(p1)):
            dis += math.pow((p1[i] - p2[i]),2)
    return math.sqrt(dis)


def cal_nc(clusters):
    pass

            
def kmeans(points, k, conv=1e-5):
    print('kmena: ', len(points))

    #--init centroid---
    
    ran_id = random.randint(0, len(points))
    start_p = points[ran_id]
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
    cur_conv = 1
    # print('--------', centroid)

    while time_ < 50 and cur_conv > conv :
        time_ += 1
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

        centroid = comp(new_centroid, len(centroid))

        
        
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
def merge_list(a,b,DIM):
    n1, su1, suq1 = a
    n2, su2, suq2 = b
    n = n1 + n2 
    su = point_addition(su1, su2,1)
    suq = point_addition(suq1, suq2,2)
    flags = []
    for i in range(DIM):
        tmp = suq[i]/n - math.pow(su[i]/n,2)
        flags.append(math.sqrt(tmp) < 3*math.sqrt(DIM))
    l = [n,su,suq]
    return (all(flags), l)

#def out_2(out_path, data):
    #print("writing into out2......")
    #with open(out_path, 'w') as f:
        #f.write('round_id,nof_cluster_discard,nof_point_discard,nof_cluster_compression,nof_point_compression,nof_point_retained\n')
        #3for item in data:
         #   item = [str(x) for x in item]
          #  f.write(','.join(item) + '\n')

if __name__ == '__main__':
    
    input_path = sys.argv[1]
    n_clusters = int(sys.argv[2])
    output_file1 = sys.argv[3]
    output_file2 = sys.argv[4]
    #with open(output_file2, 'w') as f:
        #f.write('round_id,nof_cluster_discard,nof_point_discard,nof_cluster_compression,nof_point_compression,nof_point_retained\n')

    OUTPUT1 = []
    OUTPUT2 = []

    #global POINTS
    #global DIM
    #global SAMPLE_NUM
    SAMPLE_NUM = 40

    #global DS
    DS = []
    #global DS_CLUSTER
    DS_CLUSTER = []

    #global RS
    RS = []
    RS = set()
    #global RS_DICT
    RS_DICT = {}

    #global CS
    CS = []
    #global CS_CLUSTER
    CS_CLUSTER = []

    FILES = []
    for file in Path(input_path).rglob('*.txt'):
        if file.is_file():
            FILES.append(file)
    FILES = sorted(FILES)
    #FILES = sorted(os.listdir(input_path))
    print(FILES)
    for i, filename in enumerate(FILES, start=1): 
        # filename = str(filename)
        if i == 1:
            #-- init points--
            print(filename)
            points = read_file(filename)
            POINTS = points
            le = len(points[0][1])
            DIM = le - 1

            #--- init bfr ---
            if len(POINTS)//5 > SAMPLE_NUM:
                n_sample = SAMPLE_NUM
            else: n_sample = len(POINTS)//5

            sample_points = random.sample(POINTS, n_sample)
            clusters = kmeans(sample_points, n_clusters)

            for cluster in clusters.values():
                DS.append(cal_cluster_squ(cluster,DIM))
                idx = []
                for point in cluster:
                    idx.append(point[0])
                DS_CLUSTER.append(idx)
            
            #cal nonsample
            Sample_set = {point[0] for point in sample_points}
            non_sample = []
            for point in POINTS:
                if point[0] not in Sample_set:
                    non_sample.append(point)
            new_clusters = kmeans(non_sample, 3*n_clusters)
            for cluster in new_clusters.values():
                if len(cluster) == 1:
                    RS.add(cluster[0][0])
                    RS_DICT[cluster[0][0]] = cluster[0]
                else:
                    CS.append(cal_cluster_squ(cluster, DIM))
                    idx = []
                    for point in cluster:
                        idx.append(point[0])
                    CS_CLUSTER.append(idx)                

        else:
            print(filename)
            # add point
            points = read_file(filename)
            for point in points:
                sign_ds = False
                sign_cs = False
                for j in range(len(DS)):
                    #cal 
                    tmp_n, tmp_su, tmp_suq = DS[j]
                    tmp_md, tmp_sd = 0,0
                    for k in range(DIM):
                        nmt = point[1][k] - tmp_su[k]/tmp_n
                        dnmt = math.sqrt(abs(tmp_suq[k] / tmp_n - (tmp_su[k] / tmp_n) ** 2))
                        if dnmt != 0:
                           tmp_md += (nmt / dnmt) ** 2
                        else: tmp_md += nmt **2 
                        tmp_sd += dnmt
                    md = math.sqrt(tmp_md)

                    n, su, suq = DS[j]
                    if md < math.sqrt(DIM)*3:
                        n+=1
                        su = point_addition(su, point[1],1)
                        suq = point_addition(suq, point[1],2)
                        tmpr = [n,su,suq]
                        DS[j] = tmpr
                        DS_CLUSTER[j].append(point[0])
                        sign_ds = True
                        break
                if not sign_ds:
                    for j in range(len(CS)):
                        #cal
                        tmp_n, tmp_su, tmp_suq = CS[j]
                        tmp_md, tmp_sd = 0,0
                        for k in range(DIM):
                            nmt = point[1][k] - tmp_su[k]/tmp_n
                            dnmt = math.sqrt(abs(tmp_suq[k] / tmp_n - (tmp_su[k] / tmp_n) ** 2))
                            if dnmt != 0:
                                tmp_md += (nmt / dnmt) ** 2
                            else: tmp_md += nmt **2 
                            tmp_sd += dnmt
                        md = math.sqrt(tmp_md)

                        n, su, suq = CS[j]
                        if md < math.sqrt(DIM)*3:
                            n+=1
                            su = point_addition(su, point[1],1)
                            suq = point_addition(suq, point[1],2)
                            tmpr = [n,su,suq]
                            CS[j] = tmpr
                            CS_CLUSTER[j].append(point[0])
                            sign_cs = True
                            break
                if not sign_cs and not sign_ds:
                    RS.add(point[0])
                    RS_DICT[point[0]] = point
            
            print(len(RS))
            rs_point = []
            for dx in RS:
                rs_point.append(RS_DICT[dx])
            if len(RS) > len(DS*3):
                new_clusters = kmeans(rs_point, len(DS)*3)
                for cluster in new_clusters.values():
                    if len(cluster) != 1:
                        #if
                        cur_suq =  cal_cluster_squ(cluster,DIM)
                        CS.append(cur_suq)
                        idx = []
                        for point in cluster:
                            idx.append(point[0])
                        CS_CLUSTER.append(idx)
                        RS -= set(idx)
            for x in range(len(CS) -1):
                for y in range(x+1, len(CS)):
                    flag, new_cs = merge_list(CS[x], CS[y],DIM)
                    if flag:
                        CS[y] = new_cs
                        CS_CLUSTER[y].extend(CS_CLUSTER[x])
                        CS[i] = None
                        CS_CLUSTER[x] = []
                        break
            CS = [cs for cs in CS if cs]
            CS_CLUSTER = [cs for cs in CS_CLUSTER if cs]

        if i == len(FILES):
            #print('')
            for x in range(len(CS)):
                for y in range(len(DS)):
                    flag, new_ds = merge_list(CS[x], DS[y], DIM)
                    if flag:
                        DS[y] = new_ds
                        DS_CLUSTER[y].extend(DS_CLUSTER[x])
                        CS_CLUSTER[x] = []
                        break

        whole_ds = []
        whole_cs = []
        for item in DS:
            whole_ds.append(item[0])
        for item in CS:
            whole_cs.append(item[0])
        sum_ds = sum(whole_ds)
        sum_cs = sum(whole_cs)
        info = [len(DS), sum_ds, len(CS), sum_cs, len(RS)]

        OUTPUT1.append([i] + info)
        print('round:', i, info)

   
    output2 = {}
    for i, v in enumerate(DS_CLUSTER):
        for dx in v:
            output2[dx] = i
    for i, v in enumerate(CS_CLUSTER,  start=len(DS_CLUSTER)):
        for dx in v:
            output2[dx] = i
    for i, dx in  enumerate(RS,  start=len(DS_CLUSTER)+len(CS_CLUSTER)):
        output2[dx] = i

    #print(output2)
    with open(output_file1, 'w') as f:
        json.dump(output2,f)
    
    
    #print(OUTPUT1)
    #out_2(output_file2,OUTPUT1 )
    
    with open(output_file2, 'w') as f:
        f.write('round_id,nof_cluster_discard,nof_point_discard,nof_cluster_compression,nof_point_compression,nof_point_retained\n')
        for item in OUTPUT1:
            item = [str(x) for x in item]
            f.write(','.join(item) + '\n')

print("Duration: %s seconds." % (time.time()-start))
        


