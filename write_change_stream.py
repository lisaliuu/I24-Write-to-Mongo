#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  1 11:46:00 2022

@author: liuc36
"""
import urllib.parse
import csv
import pymongo
from pymongo import MongoClient
import time
from datetime import date
from datetime import datetime
import calendar

username = urllib.parse.quote_plus('i24-data')
password = urllib.parse.quote_plus('mongodb@i24')
#client = MongoClient('mongodb://%s:%s@10.2.218.56' % (username, password))
client = MongoClient(host=['localhost:27017'])
db=client["trajectories"]
col=db["test_change_stream"]

c=1

while(c<1000):
    testDoc={'x':c,'y':c}
    col.insert_one(testDoc)
    col.update_one({'x':c},{'$set':{'y':1}})
    #print('inserted '+str(c))
    time.sleep(1)
    c=c+1