#I'm terrible so I use both "" and '' all throughout this
import redis as r
import json

#port 6379 is the default
r_cli = r.StrictRedis(host='127.0.0.1', port=6379, db=0)

count = 0
starttime = ""

#this returns a list of keys that fit the pattern
byte_keys = r_cli.keys('*_*-*')
keys = [str(bk, 'utf-8') for bk in byte_keys]

for key in keys:

    #FOR WHEN KEY MAPS TO A LIST:
    #this will gets the rightmost elment off the list, then pushes it to the head of a list
    data_pt = json.loads(r_cli.rpoplpush(key, key)) 
    #get start time of element we just got so we know where to end search 
    starttime = data_pt["starttime"]
    #see if speed of this data point is > 100
    if data_pt['speed'] != "":
        if int(data_pt['speed']) > 100:
            count = count + 1

    #go to next point to continue search
    data_pt = json.loads(r_cli.rpoplpush(key, key)) 
    while data_pt['starttime'] != starttime:
        if data_pt['speed'] != "":
            if int(data_pt['speed']) > 100:
                count = count + 1
        data_pt = json.loads(r_cli.rpoplpush(key, key)) 
        
print(count)
