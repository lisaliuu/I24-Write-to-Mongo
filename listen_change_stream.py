#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  1 11:58:54 2022

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
import logging

username = urllib.parse.quote_plus('i24-data')
password = urllib.parse.quote_plus('mongodb@i24')
#client = MongoClient('mongodb://%s:%s@10.2.218.56' % (username, password))
client = MongoClient()
db=client["trajectories"]
col=db["test_change_stream"]





# try:
resume_token = None
#pipeline = [{'$match': {'operationType': 'insert'}}]
with col.watch() as stream:
    for insert_change in stream:
        print(insert_change)
        time.sleep(3)
# except pymongo.errors.PyMongoError:
#     # The ChangeStream encountered an unrecoverable error or the
#     # resume attempt failed to recreate the cursor.
#     if resume_token is None:
#         # There is no usable resume token because there was a
#         # failure during ChangeStream initialization.
#         logging.error('...')
#     else:
#         # Use the interrupted ChangeStream's resume token to create
#         # a new ChangeStream. The new stream will continue from the
#         # last seen insert change without missing any events.
#         with col.watch(
#                 pipeline, resume_after=resume_token) as stream:
#             for insert_change in stream:
#                 print(insert_change)