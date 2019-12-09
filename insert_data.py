#I'm terrible so I use both "" and '' all throughout this
import csv
import os
import redis as r
import json

#localhost is probably going to need to change
#port 6379 is the default
r_cli = r.StrictRedis(host='localhost', port=6379, db=0)

#path is going to need to change
#highways fields: 'highwayid', 'shortdirection', 'direction', 'highwayname'
with open(os.path.expanduser("./Data/highways.csv"), newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    #row is an ordered dictionary
    for row in reader:
        #key is 'h' + highwayid
        #value is the entire row of the file, nothing is left out
        r_cli.hmset('h' + row['highwayid'], row)
    print("highways.csv insertion complete!")

#freeway_stations fields: 'stationid', 'highwayid', 'milepost', 'locationtext', 'upstream', 'downstream', 'stationclass', 'numberlanes', 'latlon', 'length'
with open(os.path.expanduser("./Data/freeway_stations.csv"), newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    #row is an ordered dictionary
    for row in reader:
        #key is 's' + stationid
        #value is the entire row of the file, nothing is left out
        r_cli.hmset('s' + row['stationid'], row)
    print("freeway_station.csv insertion complete!")

#freeway_detectors fields: 'detectorid', 'highwayid', 'milepost', 'locationtext', 'detectorclass', 'lanenumber', 'stationid'
with open(os.path.expanduser("./Data/freeway_detectors.csv"), newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    #row is an ordered dictionary
    for row in reader:
        #key is 'd' + detectorid
        #value is the entire row of the file, nothing is left out
        r_cli.hmset('d' + row['detectorid'], row)
    print("freeway_detectors.csv insertion complete!")
    

#freeway_loopdata fields: detectorid,starttime,volume,speed,occupancy,status,dqflags
with open(os.path.expanduser("./Data/freeway_loopdata.csv"), newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    #row is an ordered dictionary
    for row in reader:
        #get updated date
        chars = list(row['starttime'])
        date_list = chars[5:10]
        date = ''.join(date_list)

        #name of list in redis (key) is detectorid + date ('mm-dd')
        #rpush creates a new list if none exists under that key,
        #  and pushes a new element onto the end of the list
        jsonized = json.dumps(row)
        r_cli.rpush(row['detectorid'] + '_' + date, jsonized)
    print("freeway_loopdata.csv insertion complete!")

