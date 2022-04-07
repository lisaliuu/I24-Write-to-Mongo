from pprint import pprint
import csv
from csv import DictReader
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

#writes gt trajectories sorted by ID
username = urllib.parse.quote_plus('i24-data')
password = urllib.parse.quote_plus('mongodb@i24')
client = MongoClient('mongodb://%s:%s@10.2.218.56' % (username, password))
db=client["trajectories"]
col=db["ground_truth_trajectories_test2"]

testFilePath='/isis/home/liuc36/Desktop/testdoc'
GTFilePath='/isis/home/teohz/Desktop/data_for_mongo/GT_sort_by_ID/'
TMFilePath='/isis/home/teohz/Desktop/data_for_mongo/pollute/'

    
files=['0-12min.csv','12-23min.csv','23-34min.csv','34-45min.csv','45-56min.csv','56-66min.csv','66-74min.csv','74-82min.csv','82-89min.csv']
ofiles=[]
count=0

with open('write_time_wo_index.txt','w') as wf:
    for file in files:
        
        with open (GTFilePath+file,'r') as f:
            print('reading '+file)
            reader=csv.reader(f)
            trajDict={}
            
            next(f)
            for point in reader:
                # print(point)
                # break
                if ('ID' in trajDict):
                    if(trajDict['ID']==float(point[4])):
                        trajDict['timestamp'].extend([float(point[3])])
                        trajDict['x_position'].extend([3.2808*float(point[41])])
                        trajDict['y_position'].extend([3.2808*float(point[42])])
    
                    else:
                        trajDict["first_timestamp"]=trajDict["timestamp"][0]
                        trajDict["last_timestamp"]=trajDict["timestamp"][-1]
                        trajDict["starting_x"]=trajDict["x_position"][0]
                        trajDict["ending_x"]=trajDict["x_position"][-1]
                        d=datetime.utcnow()
                        trajDict['db_write_timestamp']=calendar.timegm(d.timetuple()) #epoch unix time
                        
                        count=count+1
                            
                        if (count%50==0):
                            st=time.time()
                            col.insert_one(trajDict)
                            et=time.time()
                            elapse=et-st
                            doccount=col.count_documents({})
                            wf.write(str(doccount)+' '+str(elapse)+'\n')
                        else:
                            col.insert_one(trajDict)
                            
                        trajDict.clear()
                        
                        trajDict['ID']=float(point[4])
                        trajDict["road_segment_id"]=int(point[49])
                        trajDict["height"]=3.2808*float(point[46])
                        trajDict["width"]=3.2808*float(point[44])
                        trajDict["length"]=3.2808*float(point[45])
                        trajDict['configuration_id']=1
                        trajDict['coarse_vehicle_class']=int(point[5])
                        trajDict['fine_vehicle_class']=int(1)
                        trajDict['direction']=int(float(point[37]))
                        
                        trajDict['timestamp']=[float(point[3])]
                        trajDict['x_position']=[3.2808*float(point[41])]
                        trajDict['y_position']=[3.2808*float(point[42])]
    
                else:   
                    trajDict['ID']=float(point[4])
                    trajDict["road_segment_id"]=int(point[49])
                    trajDict["height"]=3.2808*float(point[46])
                    trajDict["width"]=3.2808*float(point[44])
                    trajDict["length"]=3.2808*float(point[45])
                    trajDict['configuration_id']=1
                    trajDict['coarse_vehicle_class']=int(point[5])
                    trajDict['fine_vehicle_class']=int(1)
                    trajDict['direction']=int(float(point[37]))
                    
                    trajDict['timestamp']=[float(point[3])]
                    trajDict['x_position']=[3.2808*float(point[41])]
                    trajDict['y_position']=[3.2808*float(point[42])]
    
        
            trajDict["first_timestamp"]=trajDict["timestamp"][0]
            trajDict["last_timestamp"]=trajDict["timestamp"][-1]
            trajDict["starting_x"]=trajDict["x_position"][-1]
            trajDict["ending_x"]=trajDict["x_position"][0]
            d=datetime.utcnow()
            trajDict['db_write_timestamp']=calendar.timegm(d.timetuple()) #epoch unix time
            col.insert_one(trajDict)
                
            f.close()
      
    wf.close()
