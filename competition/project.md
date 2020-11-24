2020 Fall competitor project
=============================
test RMSE: 1.16394 <br>
blind RMSE: 1.16487

### train.py
#### *tools*
xgboost library
    for tree algorithm <br>
surprise library
    for SVD model

#### *data set*
data/train_review.json <br>
data/user_file.json <br>
data/uavg_path.json <br>
data/bavg_path.json <br>

#### *preprocess data*
A.*for (train_review.json)*
1. used pyspark read json data, for each (u_id,b_id), claculated the average stars, then collect()
2. transfered above data to _DataFrame_ 

B. *for (user_file.json)*
1. read user_file line by line
2. for each line, defined (user_id) as key, collected other features as a list []
3. transfered non-digit features to a digit, for example: for *"friends"* feature, I calculate the _len()_; for "yalping_since", used _mktime()_ and _strptime()_ to handle format.
4. '''new_user = defaultdict(list)'''
   '''new_user[key] = [features list]'''
5. output this new user json file

C. *for user_avg.json and business_avg.sjon*
1. read into local dataframes
2. used to build new_features

#### *build models*
1. used surprise built whole_trainset *build_full_trainset()* and whole_testset *build_testset()*
2. built 3 SVD models, saved these 3 models in Local
3. in the same loop, built whole_predict dictionary {(uid,bid):[rest fetures]}
4. to process whole_predict, read each key,
    collect user_data[uid], value from whole_preduct[:3],
    used above features build new_tmp_features
5. *build data*
   data = new_featrues[:,2:]
6. *build label*
    read each line in new_tmp_features, collect(test_set[(uid,bid)])
7. *build matrix*
'''
xgb.DMatrix(data = data.values, label = labels)
'''
8. *build model*
xgb.train
9. *save_model('matrix.model')*

### predict.py
#### *preprocess data*
A. *for test_file.json*
1. pyspark read json, transfered into DataFrame, used to build trainset()
2. upload 3 SVD models, for each model, *test()* a rough predict, then collected all predictions.(*rough_preds*)

B. *for user_avg.json, business_avg.json*
1. used to build a new user features as train.py, but for each rough_preds.item()

#### *output predict*
1. transfered above new_features into DataFrame as data[:,2:]
2. xbg.DMatix(data, label=None)
3. load_model('matrix.model')
4. booster.predict(DMatrix)