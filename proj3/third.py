# Tom Lancaster, William Mass, and Trina Rutz (c) December 2019
# Primarily written by Trina with input from other members
# Group: Mama Cass and the Redis Tributers
# third.py - 3rd Query - Redis
# Single-Day Station Travel Times: Find travel time for station Foster NB 
# for 5-minute intervals for Sept 22, 2011. Report travel time in seconds.

import redis as r
import json
​
def get_foster_nb_id(s_keys):
    for s_key in s_keys:
        byte_loc_text = r_cli.hmget(s_key, 'locationtext')
        loc_text = [str(blt, 'utf-8') for blt in byte_loc_text]
        if loc_text[0] == 'Foster NB':
            byte_stat_id = r_cli.hmget(s_key, 'stationid')
            stat_id =[str(bsi, 'utf-8') for bsi in byte_stat_id]
            #pulls the length of the station
            byte_length_foster = r_cli.hmget(s_key, 'length')
            length_foster = [str(blf, 'utf-8') for blf in byte_length_foster]
            foster_length = float(length_foster[0])
            return stat_id[0], foster_length
​
def get_detector_id_list_for_foster_nb(d_keys):
    foster_nb_detector_keys = []
    for d_key in d_keys:
        byte_dec_id = r_cli.hmget(d_key, 'stationid')
        dec_id = [str(bdi, 'utf-8') for bdi in byte_dec_id]
        if dec_id[0] == foster_nb_id:
            foster_nb_detector_keys.append(d_key)
    return foster_nb_detector_keys
​
#port 6379 is the default
r_cli = r.StrictRedis(host='127.0.0.1', port=6379, db=0)
​
#get station id for Foster NB
byte_s_keys = r_cli.keys('s1*')
s_keys = [str(bk, 'utf-8') for bk in byte_s_keys]
foster_nb_id, foster_length = get_foster_nb_id(s_keys)
​
#get detector ids that correspond to station id for foster NB
byte_d_keys = r_cli.keys('d*')
d_keys = [str(bk, 'utf-8') for bk in byte_d_keys]
foster_nb_detector_keys = get_detector_id_list_for_foster_nb(d_keys)
​
#make correct loopdata key for detectors found above
foster_nb_loopdata_keys = []
for key in foster_nb_detector_keys:
    #dropping the d in key, so our next query doesn't fail
    list_key = list(key)
    drop_d_in_key = list_key[1:]
    number_key = ''.join(drop_d_in_key) 
    foster_nb_loopdata_keys.append(number_key + '_09-22')
​
#Query redis with loopdata keys to find volumes
#The redis query used here pops a value off the right side of the list 
#  returns it to the user, and pushes it to the head of the list
#To avoid an infinite loop, we keep track of the first start time (guarenteed to be unique) 
#  and stop searching for new elements when we get back to that same time
total_vol = 0
starttime = ''
row_count = 0
total_travel_times = []
for key in foster_nb_loopdata_keys:
    speeds = []
    car_count = 0
    vol = 0
    data_pt = json.loads(r_cli.rpoplpush(key, key)) 
    row_count += 1
    starttime = data_pt["starttime"]
​
    vol = data_pt['volume']
    if data_pt['speed'] != "" and int(data_pt['speed']) > 5:
        speeds.append(int(data_pt['speed']) * int(vol))
        car_count += int(vol)
    data_pt = json.loads(r_cli.rpoplpush(key, key)) 
    row_count += 1
​
    while data_pt['starttime'] != starttime:
        vol = data_pt['volume']
        if data_pt['speed'] != "" and int(data_pt['speed']) > 5:
            speeds.append(int(data_pt['speed']) * int(vol)) 
            car_count += int(vol)
        if row_count == 15:
            row_count = 0
            if len(speeds) > 0:
                ave_speed = sum(speeds) / car_count 
                travel_time = foster_length / ave_speed * 3600
                total_travel_times.append(travel_time)
            speeds = []
            car_count = 0
        data_pt = json.loads(r_cli.rpoplpush(key, key)) 
        row_count += 1
​
stuff = [print(x) for x in total_travel_times]