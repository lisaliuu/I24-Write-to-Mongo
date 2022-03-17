from pprint import pprint
import csv
import pandas as pd
import pymongo
from pymongo import MongoClient
import numpy as np
import bson
import time
from datetime import date
from datetime import datetime
import calendar
import urllib.parse

#lastDocument=['{ "_id" : ObjectId("6233513c663d8f4240e82c57"), "ID" : NumberLong(1441) }']

username = urllib.parse.quote_plus('i24-data')
password = urllib.parse.quote_plus('mongodb@i24')
client = MongoClient('mongodb://%s:%s@10.2.218.56' % (username, password))
db=client["trajectories"]
col=db["ground_truth_trajectories"]

GTFilePath='/isis/home/teohz/Desktop/data_for_mongo/GT/'

GTschema=["timestamp","ID","Coarse_vehicle_class","direction","x_position","y_position","width","length","height","configuration_ID","road_segment_ID","x_velocity","y_velocity","x_acceleration","y_acceleration","x_jerk","y_jerk"]

def generate_single_GTtrajectory(df_car):
    
    df_list=pd.DataFrame()

    df_list["timestamp"]=df_car["timestamp"]
    df_list["x_position"]=to_feet(df_car["x_position"])
    df_list["y_position"]=to_feet(df_car["y_position"])
    df_list["x_velocity"]=to_feet(df_car["x_velocity"])
    df_list["y_velocity"]=to_feet(df_car["y_velocity"])
    df_list["x_acceleration"]=to_feet(df_car["x_acceleration"])
    df_list["y_acceleration"]=to_feet(df_car["y_acceleration"])
    df_list["x_jerk"]=to_feet(df_car["x_jerk"])
    df_list["y_jerk"]=to_feet(df_car["y_jerk"])

    listdict=df_list.to_dict("list") #turn into arrays

    listdict['ID']=bson.Int64(df_car['ID'].iat[0])
    listdict['coarse_vehicle_class']=bson.Int64(df_car['Coarse_vehicle_class'].iat[0])
    listdict["first_timestamp"]=(df_car['timestamp'].iat[0])
    listdict["last_timestamp"]=(df_car['timestamp'].iat[-1])
    listdict["starting_x"]=(to_feet(df_car['x_position'].iat[0]))
    listdict["ending_x"]=(to_feet(df_car['x_position'].iat[-1]))
    listdict["road_segment_ID"]=bson.Int64(df_car["road_segment_ID"].iat[0])
    listdict["height"]=(to_feet(df_car["height"].iat[0]))
    listdict["width"]=(to_feet(df_car["width"].iat[0]))
    listdict["length"]=(to_feet(df_car["length"].iat[0]))
    listdict['configuration_ID']=1 #other single values

    d=datetime.utcnow()
    listdict['db_write_timestamp']=calendar.timegm(d.timetuple()) #epoch unix time

    col.insert_one(listdict) #write dictionaries to MongoDB

    return

def generate_single_TMtrajectory(df_car):
    
    df_list=pd.DataFrame()

    df_list["timestamp"]=df_car["timestamp"]
    df_list["x_position"]=to_feet(df_car["x_position"])
    df_list["y_position"]=to_feet(df_car["y_position"])
    df_list["height"]=to_feet(df_car["height"])
    df_list["width"]=to_feet(df_car["width"])
    df_list["length"]=to_feet(df_car["length"])

    listdict=df_list.to_dict("list")

    listdict['ID']=bson.Int64(df_car['ID'].iat[0])
    listdict['coarse_vehicle_class']=bson.Int64(df_car['Coarse_vehicle_class'].iat[0])
    listdict["first_timestamp"]=(df_car['timestamp'].iat[0])
    listdict["last_timestamp"]=(df_car['timestamp'].iat[-1])
    listdict["starting_x"]=(to_feet(df_car['x_position'].iat[0]))
    listdict["ending_x"]=(to_feet(df_car['x_position'].iat[-1]))
    listdict["road_segment_ID"]=bson.Int64(df_car["road_segment_ID"].iat[0])
    listdict['configuration_ID']=1

    d=datetime.utcnow()
    listdict['db_write_timestamp']=calendar.timegm(d.timetuple())

    col.insert_one(listdict)
 
    return

def to_feet(meters): #turn positions to feet
    return meters*3.2808

df = pd.read_csv(GTFilePath+'12-23min.csv',usecols=GTschema,skiprows=10000000,nrows=10000000)
df.groupby(["ID"],as_index=False).apply(generate_single_GTtrajectory) #group and write to database

#ld= db.col.find({},{'ID':1}).sort({'_id':-1}).limit(1)

#lastDocument.append(ld)
#are vel, acc, jerk in meters too
