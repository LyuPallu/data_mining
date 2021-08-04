import sys
import time
import pyspark
#import os
#os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")
from operator import add
from pyspark import SparkContext, SQLContext, StorageLevel, SparkConf
from pyspark.sql import functions
#from graphframes import *
from itertools import combinations
from collections import defaultdict
from operator import add
from copy import deepcopy

#--------- functions-----------------
def Jac(item,matrix):
    u1 = set(matrix[item[0]])
    u2 = set(matrix[item[1]])
    inter_ = len(u1 & u2)
    union_ = len(u1 | u2)
    jac = inter_ / union_
    return jac

def gen_g(data):
    un_nodes = list(data.keys())
    g = defaultdict(set)
    for pair in combinations(un_nodes,2):
        if case_number == 2:
            jac = Jac(pair,data)
            if jac >= 0.5:
                g[pair[0]].add(pair[1])
                g[pair[1]].add(pair[0])
        if case_number == 1:
            g[pair[0]].add(pair[1])
            g[pair[1]].add(pair[0])
    return g

def gen_btw(g, nodes):
    btws = defaultdict(set)

    for x in nodes:
        parent = defaultdict(set)
        parent[x] = []

        lv = 1

        depth = {x:0}
        #depth = ({i:1} for i in g[x] )

        stack = []
        stack.append(x)
        #stack = [i for i in g[x]]

        for item in g[x]:
            if x not in parent[item]:
                parent[item].add(x)

            depth[item] = 1
            stack.append(item)

        
    
        path = {x:1}
        crd = {x:0.0}
        queue = [i for i in g[x]]
        while queue:
            node = queue.pop(0)
            crd[node] = lv
            path[node] = sum([path[i] for i in parent[node]])

            #queue.append(n for n in g[node] if n not in stack)
            
            for n in g[node]:
                level = depth[node] + lv

                #depth[n] = (depth[node] + lv if n not in stack else 0)
                #queue = [n]
                if n not in stack:
                    parent[n] = [node]
                    depth[n] = level
                    queue.append(n)
                    stack.append(n)

                elif depth[n] == level:
                        parent[n].append(node)

            #queue.append(n)

        b_tmp = {}
        #-credit---
        for q in reversed(stack[1:]):
            for p in parent[q]:
                lv = (path[p]/path[q])
                tmp = crd[q] * lv
                crd[p] += tmp
                b_tmp[tuple(sorted([q,p]))] = tmp
        #-- btw ---
        for item in b_tmp.items():
            if item[0] in btws:
                btws[item[0]] += item[1]
            else: btws[item[0]] = item[1]

    result = {}
    result = btws
    return result

def gen_com(g,nodes,demon,ori_btws):
    #ori_btws = deepcopy(btws)
    ori_g = deepcopy(g)

    queue = []
        
    can_MOD = -1
    max_com = []
                
    m = len(btws)
    
    #max_btws = max(ori_btws.values())
    while ori_btws:
        nodes_set = deepcopy(nodes)

        
        com = []
        while nodes_set:
            queue = [nodes_set.pop()]
            tmp_com = []
            for q in queue:
                if q not in tmp_com:
                    tmp_com.append(q)
                for node in g[q]:
                    if node not in tmp_com: 
                        queue.append(node)

            com.append(sorted(tmp_com))

            nodes_set -= set(tmp_com)
        
        #--- modularity ---
        Q = 0
        for c in com:
            for j in c:
                for k in c: 
                    A = (1 if k in ori_g[j] else 0)
                    tmp_j = len(ori_g[j])
                    tmp_k = len(ori_g[k])
                    Q += (A - (tmp_j*tmp_k)/demon)
        Q = Q / (m*2)

        if -1 < Q < 1:
            if Q > can_MOD:
                can_MOD = Q
                max_com = com

        #-- iterate---------
        for u,v in ori_btws.items():
            #u = i[0]
            if v == max(ori_btws.values()):
                if u[0] in g[u[1]]:
                    g[u[1]].remove(u[0])
                if u[1] in g[u[0]]:
                    g[u[0]].remove(u[1])
        ori_btws = gen_btw(g,nodes)

    result = []
    result = max_com
    return result

start = time.time()

sc = SparkContext(appName="task2")

_, case_number, input_file, btw_output_file, com_output_file = sys.argv

case_number = int(case_number)

lines = sc.textFile(input_file).filter(lambda x : x != ('user_id,state'))\
        .map(lambda x: x.split(','))

if case_number == 1:
    #---------- prepare -----------
    #(user,[s1,s2,s3])
    us = lines.map(lambda x: (x[0], x[1]))\
                    .reduceByKey(lambda u, v: u + v)\
                    .mapValues(set)\
                    .collectAsMap()
    #(state, [u1,u2,u3])
    su = lines.map(lambda x: (x[1], [x[0]]))\
                    .reduceByKey(lambda u, v: u + v)\
                    .mapValues(set).collect()
                   
    for item in su:
        if item[0] in us:
            us[item[0]] += item[1]
        else: us[item[0]] = item[1]

    un_nodes = list(us.keys())
    g = defaultdict(set)
    for pair in combinations(un_nodes,2):
            g[pair[0]].add(pair[1])
            g[pair[1]].add(pair[0])

    nodes = sc.parallelize(g.keys()).collect()
    nodes = set(sorted(nodes)) 

    #---------- betweenness -------
    btws = gen_btw(g,nodes)
    for b in btws.items():
        btws[b[0]] = b[1]/2
    btwsness = sorted(btws.items(), key=lambda x:((-1)*x[1],x[0]))
    btwsness = sc.parallelize(list(btwsness)).collect()
    #print(btwsness)
    with open(btw_output_file, 'w') as f:
        for i in btwsness:
            f.write(str(i[0]) + ' , ' + str(i[1]))
            f.write('\n')
    
    #---------- communities -------------
    demon_1 = len(btws)
    com = gen_com(g,nodes,demon_1,btws)
    commu = sorted(com, key=lambda x: len(x))
    #print(commu)

    with open(com_output_file,'w') as f:
        for i in commu:
            f.write(str(i)[1:-1])
            f.write('\n')

#---------------------------------
if case_number == 2:
    data = lines.map(lambda x: (x[0],[x[1]])).reduceByKey(lambda u, v: u + v)\
                .mapValues(set).collectAsMap()
    g= gen_g(data) 
    nodes = sc.parallelize(g.keys()).collect()
    nodes = set(sorted(nodes))  
    
    #---------- betweenness ----------
    btws = gen_btw(g, nodes)
    for b in btws.items():
        btws[b[0]] = b[1]/2
    btwsness = sorted(btws.items(), key=lambda x:((-1)*x[1],x[0]))

    btwsness = sc.parallelize(list(btwsness)).collect()
    print(type(btwsness))

    with open(btw_output_file, 'w') as f:
        for i in btwsness:
            f.write(str(i[0])+', '+str(i[1]))
            #f.write(str(i[0])+','+str(i[1]))
            f.write('\n')

    #---------- communities -------------
    demon_2 = len(btws)*2
    com = gen_com(g,nodes,demon_2,btws)
    commu = sorted(com, key=lambda x: len(x))
    
   #print(commu)
    with open(com_output_file,'w') as f:
        for i in commu:
            f.write(str(i)[1:-1])
            f.write('\n')


print("Duration: %s seconds." % (time.time()-start))