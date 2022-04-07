#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 30 22:45:26 2022

@author: liuc36
"""
#add a new field, fragment_ids, in ground_truth that contains corresponding fragment ids of raw_trajectories

import urllib.parse
import csv
import pymongo
from pymongo import MongoClient


username = urllib.parse.quote_plus('i24-data')
password = urllib.parse.quote_plus('mongodb@i24')
client = MongoClient('mongodb://%s:%s@10.2.218.56' % (username, password))
db=client["trajectories"]
colraw=db["raw_trajectories"]
colgt=db['ground_truth_trajectories_test']

for rawdoc in colraw.find({}):

    _id=rawdoc.get('_id')
    raw_ID=rawdoc.get('ID')
    gt_ID=raw_ID//100000
    colgt.update_one({'ID':gt_ID},{'$push':{'fragment_ids':_id}},upsert=True)
       # print(docgt)
    #print(gt_ID)
