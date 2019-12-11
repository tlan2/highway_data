# Tom Lancaster, William Mass, and Trina Rutz (c) December 2019
# Coded by Will who imported lots of code from other queries primarily coded by Trina
# Group: Mama Cass and the Redis Tributers 
# fourth.py - 4th Query - Redis
# Peak Period Travel Times: Find the average travel time for 7-9AM and 4-6PM on September
# 22, 2011 for station Foster NB. Report travel time in seconds.

import redis as r
import json
import datetime

def get_foster_nb_id(s_keys):
    for s_key in s_keys:
        byte_loc_text = r_cli.hmget(s_key, 'locationtext')
        loc_text = [str(blt, 'utf-8') for blt in byte_loc_text]
        if loc_text[0] == 'Foster NB':
            byte_stat_id = r_cli.hmget(s_key, 'stationid')
            stat_id =[str(bsi, 'utf-8') for bsi in byte_stat_id]
            #pulls the length of the station from the stations file for travel time calculations
            byte_length_foster = r_cli.hmget(s_key, 'length')
            length_foster = [str(blf, 'utf-8') for blf in byte_length_foster]
            foster_length = float(length_foster[0])
    return stat_id[0], foster_length

def get_detector_id_list_for_foster_nb(d_keys):
    foster_nb_detector_keys = []
    for d_key in d_keys:
        byte_dec_id = r_cli.hmget(d_key, 'stationid')
        dec_id = [str(bdi, 'utf-8') for bdi in byte_dec_id]
        if dec_id[0] == foster_nb_id:
            foster_nb_detector_keys.append(d_key)
    return foster_nb_detector_keys

#execution time stamp
print("Query began execution at %s" % (str(datetime.datetime.now())))

#port 6379 is the default
r_cli = r.StrictRedis(host='127.0.0.1', port=6379, db=0)

#get station id for Foster NB
byte_s_keys = r_cli.keys('s1*')
s_keys = [str(bk, 'utf-8') for bk in byte_s_keys]
foster_nb_id, foster_length = get_foster_nb_id(s_keys)

#get detector ids that correspond to station id for foster NB
byte_d_keys = r_cli.keys('d*')
d_keys = [str(bk, 'utf-8') for bk in byte_d_keys]
foster_nb_detector_keys = get_detector_id_list_for_foster_nb(d_keys)

#make correct loopdata key for detectors found above
foster_nb_loopdata_keys = []
for key in foster_nb_detector_keys:
    #dropping the d in key, so our next query doesn't fail
    list_key = list(key)
    drop_d_in_key = list_key[1:]
    number_key = ''.join(drop_d_in_key) 
    foster_nb_loopdata_keys.append(number_key + '_09-22')

#query redis with loopdata keys to find speed 
morning_speed_total = 0
evening_speed_total = 0
morning_volume = 0
evening_volume = 0
starttime = ''
for key in foster_nb_loopdata_keys:
    data_pt = json.loads(r_cli.rpoplpush(key, key)) 
    starttime = data_pt["starttime"]
    morning_rush_hour = ["07", "08"] #From 7:00 AM to 8:59 AM
    evening_rush_hour = ["16", "17"] #From 4:00 PM to 5:59 PM
    if data_pt["speed"] != "":
        if int(data_pt["speed"]) > 5:
            time_stamp_hour = str(data_pt["starttime"])
            time_stamp_hour = time_stamp_hour[11:13]
            speed = int(data_pt["speed"])
            volume = int(data_pt["volume"])
            if time_stamp_hour in morning_rush_hour:
                morning_speed_total += (volume * speed)
                morning_volume += volume
            elif time_stamp_hour in evening_rush_hour:
                evening_speed_total += (speed * volume)
                evening_volume += volume

    data_pt = json.loads(r_cli.rpoplpush(key,key))

    while data_pt["starttime"] != starttime:
        if data_pt["speed"] != "":
            if int(data_pt["speed"]) > 5:
                time_stamp_hour = str(data_pt["starttime"])
                time_stamp_hour = time_stamp_hour[11:13]
                speed = int(data_pt["speed"])
                volume = int(data_pt["volume"])
                if time_stamp_hour in morning_rush_hour:
                    morning_speed_total += (speed * volume)
                    morning_volume += volume
                elif time_stamp_hour in evening_rush_hour:
                    evening_speed_total += (speed * volume)
                    evening_volume += volume
        data_pt = json.loads(r_cli.rpoplpush(key, key))

am_speed = (foster_length / (morning_speed_total / morning_volume)) * 3660
pm_speed = (foster_length / (evening_speed_total / evening_volume)) * 3660

print("Morning Total Speed: %s  --  Evening Total Speed: %s" % (morning_speed_total, evening_speed_total))
print("Morning Total Volume %s  --  Evening Total Volume: %s" % (morning_volume, evening_volume))

print("Morning Rush Hour Average Travel Time in Seconds: %s" % (am_speed))
print("Evening Rush Hour Average Travel Time in Seconds: %s" % (pm_speed))

print("Query completed at %s" % (str(datetime.datetime.now())))
