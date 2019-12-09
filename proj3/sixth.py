#Route Finding: Find a route from Johnson Creek to Columbia Blvd on I-205 NB using the upstream and downstream fields.
import redis as r
import json

def get_station_ids_from_text(s_keys):
    johnson_id = ''
    columbia_id = ''
    for s_key in s_keys:
        byte_loc_text = r_cli.hmget(s_key, 'locationtext')
        loc_text = [str(blt, 'utf-8') for blt in byte_loc_text]
        if loc_text[0] == 'Johnson Cr NB':
            byte_stat_id = r_cli.hmget(s_key, 'stationid')
            stat_id =[str(bsi, 'utf-8') for bsi in byte_stat_id]
            johnson_id = stat_id[0]
        if loc_text[0] == 'Columbia to I-205 NB':
            byte_stat_id = r_cli.hmget(s_key, 'stationid')
            stat_id =[str(bsi, 'utf-8') for bsi in byte_stat_id]
            columbia_id = stat_id[0]
    return int(johnson_id), int(columbia_id)

#algorithm to return route from johnson to columbia
#  finds route from columbia to johnson, then returns reversed route
def get_route(start_id, end_id, stations, route):
    if start_id == end_id:
        return reversed(route)
    for station in stations:
        if start_id == int(station['downstream']):
            route.append(station['locationtext'])
            get_route(int(station['stationid']), end_id, stations, route)
            break
    return reversed(route)

#-----------------------------------------------------------------------------
#port 6379 is the default
r_cli = r.StrictRedis(host='127.0.0.1', port=6379, db=0)

#gets johnson and columbia station ids, using text
byte_s_keys = r_cli.keys('s1*')
s_keys = [str(bsk, 'utf-8') for bsk in byte_s_keys]
johnson_id, columbia_id = get_station_ids_from_text(s_keys)

#get johnson's milepost
byte_johnson_milepost = r_cli.hmget('s' + str(johnson_id), 'milepost')
list_johnson_milepost = [str(bjm, 'utf-8') for bjm in byte_johnson_milepost]
johnson_milepost = float(list_johnson_milepost[0])

#get columbia's milepost
byte_columbia_milepost = r_cli.hmget('s' + str(columbia_id), 'milepost')
list_columbia_milepost = [str(bcm, 'utf-8') for bcm in byte_columbia_milepost]
columbia_milepost = float(list_columbia_milepost[0])

#divide station info by northbound and southbound
nb_stations = []
sb_stations = []
for s_key in s_keys:
    byte_highway_id = r_cli.hmget(s_key, 'highwayid')
    list_highway_id = [str(bhi, 'utf-8') for bhi in byte_highway_id]
    highway_id = int(list_highway_id[0])

    byte_station_dict = r_cli.hgetall(s_key)
    station_dict = {}
    for key in byte_station_dict:
        station_dict[str(key, 'utf-8')] = str(byte_station_dict[key], 'utf-8')
    if highway_id == 3:
        nb_stations.append(station_dict)
    else:
        sb_stations.append(station_dict)

#see if johnson creek is north or south of columbia by checking milepost
#  if milepost of johnson is higher, you travel NORTH from johnson creek to get to columbia
#  then calculate route using either nb or sb station data
route = []
route.append('Columbia to I-205 NB')
if johnson_milepost < columbia_milepost:
    route = get_route(columbia_id, johnson_id, nb_stations, route)
else:
    route = get_route(columbia_id, johnson_id, sb_stations, route)

#prints	location text for route from johnson to columbia
for row in route:
    print(row)
