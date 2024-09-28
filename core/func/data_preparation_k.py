# %% [markdown]
# Data Preparation for Optimizer

# %%

import pandas as pd
import sys
# sys.path.append('../core/database')
# import config
# from sqlalchemy import create_engine
from core.database.sessions import engine
import itertools
from geopy.distance import geodesic as gd
import copy
import time
import datetime
import numpy as np
from typing import List,Callable,Tuple
from collections import namedtuple
from functools import partial
# import import_ipynb

# %% [markdown]
# load_dotenv()
# db = os.getenv('db')
# user = os.getenv('user')
# password = getpass('Password : ')
# host = os.getenv('host')
# port = os.getenv('port')

# %%
# SQLALCHEMY_DB_URL = config.settings.POSTGRES_URL
# engine = create_engine(SQLALCHEMY_DB_URL)

# %%
def retrieve_port(param):
    sql = 'select * FROM rms_pelni.master_port'
    df = pd.read_sql(sql, engine)

    df_port = df[df["is_deleted"] == False]
    # df_port = df_port[df_port['latitude'].isna()==False]
    # df_port = df_port[df_port['longitude'].isna()==False]
    df_port = df_port[df_port['flag_ppss'] == 'Y']
    df_port = df_port.drop_duplicates()
    df_port = df_port[['id_port', 'code', 'port_code', 'port_name', 'flag_pioneer', 
                  'height_at_high_tide', 'height_at_low_tide', 'wharf_depth',
                  'wharf_length', 'latitude', 'longitude', 'timezone_offset',
                  'max_berth_time', 'min_berth_time', 'number_of_berth', 'avg_berth_time']]
    df_port = df_port.rename(columns={'id_port' : 'id', 'port_code' : 'portcode', 
                                      'port_name' : 'name', 'flag_pioneer' : 'homebase', 
                                      'height_at_high_tide' : 'hightide', 
                                      'height_at_low_tide' : 'lowtide', 
                                      'wharf_depth' : 'depth', 
                                      'wharf_length' : 'length', 
                                      'latitude' : 'lat', 'longitude' : 'long', 
                                      'timezone_offset' : 'region', 
                                      'max_berth_time' : 'maxberthtime', 
                                      'min_berth_time' : 'minberthtime', 
                                      'number_of_berth' : 'noberth', 
                                      'avg_berth_time':'avgberth'})
    df_port.insert(0, 'portno', df_port.index)
    set_port = df_port['code']

    if param == ('001'):
        return df_port
    elif param == ('002'):
        return set_port

# %%
# def cleaned_port(df_port):
#     port = copy.deepcopy(df_port)
    
#     port['depth'] = port['depth'].astype(str).str.extract('(\d+)')
#     port.loc[((port['depth'] == '') | (port['depth'] == None) | (port['depth'].isna())), 'depth'] = 10
#     port['depth'] = port['depth'].astype(float)

#     port['length'] = port['length'].astype(str).str.extract('(\d+)')
#     port.loc[((port['length'] == '') | (port['length'] == None) | (port['length'].isna())), 'length'] = 200
#     port['length'] = port['length'].astype(float)

#     port['avgberth'] = port['avgberth'].astype(str).str.extract('(\d+)').astype(float)
#     port.loc[((port['avgberth'] == '') | (port['avgberth'] == None) | (port['avgberth'].isna())), 'avgberth'] = 2
    
#     port['lat'] = port['lat'].astype(str).str.extract('(\d+)')
#     port.loc[((port['lat'] == '') | (port['lat'] == None) | (port['lat'].isna())), 'lat'] = 24
#     port['lat'] = port['lat'].astype(float)

#     port['long'] = port['long'].astype(str).str.extract('(\d+)')
#     port.loc[((port['long'] == '') | (port['long'] == None) | (port['long'].isna())), 'long'] = (-96)
#     port['long'] = port['long'].astype(float)
    
#     port['noberth'] = port['noberth'].astype(str).str.extract('(\d+)')
#     port.loc[((port['noberth'] == '') | (port['noberth'] == None) | (port['noberth'].isna())), 'noberth'] = 1
#     port['noberth'] = port['noberth'].astype(int)

#     minberthtime = '00:00:00'
#     minberthtime = datetime.strptime(minberthtime, '%H:%M:%S')
#     port.loc[((port['minberthtime'] == 0) | (port['minberthtime'] == None) | (port['minberthtime'].isna())), 'minberthtime'] = minberthtime
#     port['minberthtime'] = pd.to_datetime(port['minberthtime'])
#     port['minberthtime'] = port['minberthtime'].dt.time
        
#     maxberthtime = '23:59:59'
#     maxberthtime = datetime.strptime(maxberthtime, '%H:%M:%S')
#     port.loc[((port['maxberthtime'] == 0) | (port['maxberthtime'] == None) | (port['maxberthtime'].isna())), 'maxberthtime'] = maxberthtime
#     port['maxberthtime'] = pd.to_datetime(port['maxberthtime'])
#     port['maxberthtime'] = port['maxberthtime'].dt.time

#     return port

# %%
def retrieve_ship(param):
    sql = 'select * FROM rms_pelni.master_ship'
    df = pd.read_sql(sql, engine)

    df_ship = df[df["is_deleted"] == False]
    df_ship = df_ship.drop_duplicates()
    df_ship = df_ship[['id_ship', 'ship_code', 'name', 'speed', 'fuel_need',
                       'capacity_pax', 'capacity_cargo', 'capacity_container_dry',
                       'capacity_container_reefer', 'capacity_vehicle_truck', 
                       'capacity_vehicle_car', 'capacity_vehicle_motorcycle', 
                       'capacity_redpack', 'length_overal','draught']]
    df_ship = df_ship.rename(columns={'id_ship' : 'id', 'ship_code' : 'code', 
                                      'fuel_need' : 'fuel', 
                                      'capacity_pax' : 'capacitypax', 
                                      'capacity_cargo' : 'capacitycargo', 
                                      'capacity_container_dry' : 'capacitydry', 
                                      'capacity_container_reefer' : 'capacityreefer', 
                                      'capacity_vehicle_truck' : 'capacitytruck', 
                                      'capacity_vehicle_car' : 'capacitycar', 
                                      'capacity_vehicle_motorcycle' : 'capacitymotor', 
                                      'capacity_redpack' : 'capacityredpack', 
                                      'length_overal' : 'length',
                                      'draught':'draft'})
    df_ship.insert(0, 'shipno', df_ship.index)
    set_ship = df_ship['code']

    if param == ('001'):
        return df_ship
    elif param == ('002'):
        return set_ship

# %%
def retrieve_portdistance():
    sql = 'select * FROM rms_pelni.master_port_distance'
    df = pd.read_sql(sql, engine)

    df_portdistance = df[df['is_deleted'] ==  False]
    df_portdistance = df_portdistance.drop_duplicates()
    df_portdistance = df_portdistance[['code_origin', 'code_destination', 'nautical', 'commercial', 
                                       'port_origin', 'port_destination']]
    df_portdistance = df_portdistance.rename(columns={'code_origin' : 'origin', 
                                                      'code_destination' : 'destination', 
                                                      'port_origin' : 'id_origin', 
                                                      'port_destination' :'id_destination'})
    return df_portdistance

# %%
def retrieve_revenue(typerev):
    sql =  f'select * from rms_pelni.hist_pax_revenue hpr where type_rev = {typerev}'
    df = pd.read_sql(sql, engine)

    #df_revenue = df[df['type_rev'] == type_rev]
    df_revenue = df[df['is_deleted'] == False]
    df_revenue = df_revenue.drop_duplicates()
    df_revenue = df_revenue[['origin_port', 'destination_port', 'departure_date',
                             'departure', 'revenue', 'total', 'type']]
    df_revenue = df_revenue.rename(columns={'origin_port' : 'origin', 
                                            'destination_port' : 'destination', 
                                            'departure_date' : 'depdate', 
                                            'departure' : 'deptime'})
    return df_revenue

# %%
def retrieve_maintenance():
    sql = 'select * FROM rms_pelni.master_ship_maintenance'
    df = pd.read_sql(sql, engine)

    df_maintenance = df[df['is_deleted'] == False]
    df_maintenance = df_maintenance.drop_duplicates()
    df_maintenance = df_maintenance[['id_ship', 'shipyard_code', 'start_docking_date',
                                    'end_docking_date', 'actual_start_docking_date',
                                    'actual_end_docking_date']]
    return df_maintenance

# %%
def retrieve_tide():
    sql = 'select * FROM rms_pelni.master_tide'
    df = pd.read_sql(sql, engine)

    df_tide = df[df['is_deleted'] == False]
    df_tide = df_tide[['id_port', 'start_date_time', 'end_date_time',
                       'tide', 'min_tide', 'max_tide', 'avg_tide']]
    df_tide = df_tide.rename(columns={'id_port' : 'idport', 
                                      'start_date_time' : 'start', 
                                      'end_date_time' : 'end', 
                                      'min_tide' : 'mintide', 
                                      'max_tide' : 'maxtide', 
                                      'avg_tide' : 'avgtide'})
    df_tide['start'] = pd.to_datetime(df_tide['start'])
    df_tide['end'] = pd.to_datetime(df_tide['end'])
    
    return df_tide

# %%
def retrieve_priceconfig():
    sql = 'select * FROM rms_pelni.price_config'
    df = pd.read_sql(sql, engine)

    df_priceconfig = df[df['is_deleted'] == False]
    df_priceconfig = df_priceconfig.drop_duplicates()
    df_priceconfig = df_priceconfig[['min_distance', 'max_distance', 'coefficient',
                                     'distance_coeff', 'pangsa']]
    df_priceconfig = df_priceconfig.rename(columns={'min_distance' : 'mindistance', 
                                                    'max_distance' : 'maxdistance',
                                                    'coefficient' : 'coeff', 
                                                    'distance_coeff' : 'distancecoeff'})
    return df_priceconfig

# %%
def retrieve_basefare():
    sql = 'select * FROM rms_pelni.basefare_config'
    df = pd.read_sql(sql, engine)

    df_basefare = df[df['is_deleted'] == False]
    df_basefare = df_basefare[df_basefare['status_active'] == 'ACTIVE']
    df_basefare = df_basefare.drop_duplicates()
    df_basefare = df_basefare[['start_date', 'end_date', 'status_active',
                               'type', 'base_fare', 'type_fare']]
    df_basefare = df_basefare.rename(columns={'start_date' : 'startdate', 
                                              'end_date' : 'enddate',
                                              'base_fare' : 'basefare',
                                              'type_fare' : 'typefare'})
    return df_basefare

# %%
def retrieve_rulecost():
    sql = 'select * FROM rms_pelni.rule_cost'
    df = pd.read_sql(sql, engine)

    df_rulecost = df[df["is_deleted"] == False]
    df_rulecost = df_rulecost.drop_duplicates()
    df_rulecost = df_rulecost[['id_rule_cost','name', 'pax', 'sailing_time', 'berthing_time', 
                               'local_time', 'id_port', 'id_ship', 'cost', 'expenses_day']]
    df_rulecost = df_rulecost.rename(columns={'id_rule_cost':'idrulecost', 'pax' : 'ispax', 
                                              'sailing_time' : 'issailing',  
                                              'berthing_time' : 'isberthing',
                                              'local_time' : 'localtime',
                                              'id_port' : 'idport', 
                                              'id_ship' : 'idship', 
                                              'expenses_day':'perday'})
    return df_rulecost

# %%
def retrieve_lowpeak():
    sql = 'select * FROM rms_pelni.master_low_peak'
    df = pd.read_sql(sql, engine)

    df_lowpeak = df[df['is_deleted'] ==  False]
    df_lowpeak = df_lowpeak.drop_duplicates()
    df_lowpeak = df_lowpeak[['name', 'type', 'start_date', 'end_date']]
    df_lowpeak = df_lowpeak.rename(columns={'start_date' : 'startdate', 
                                            'end_date' : 'enddate'})
    return df_lowpeak

# %%
def retrieve_pricecargoconfig():
    sql = 'select * FROM rms_pelni.price_cargo_config'
    df = pd.read_sql(sql, engine)

    df_pricecargoconfig = df[df['is_deleted'] ==  False]
    df_pricecargoconfig = df_pricecargoconfig.drop_duplicates()
    df_pricecargoconfig = df_pricecargoconfig[['min_distance', 'max_distance',
                                               'constant', 'distance_constant']]
    df_pricecargoconfig = df_pricecargoconfig.rename(columns={'min_distance' : 'mindistance', 
                                                              'max_distance' : 'maxdistance',
                                                              'constant' : 'coeff', 
                                                              'distance_constant' : 'distancecoeff'})
    return df_pricecargoconfig

# %%
def retrieve_adjustment():
    sql = 'select * FROM rms_pelni.adjustment_config'
    df = pd.read_sql(sql, engine)

    df_adjustment = df[df['is_deleted'] == False]
    df_adjustment = df_adjustment.drop_duplicates()
    
    return df_adjustment.value.iloc[0]

# %%
def retrieve_cargoflat():
    sql = 'select * FROM rms_pelni.cargo_flat_config'
    df = pd.read_sql(sql, engine)
    df['is_deleted'] = df['is_deleted'].fillna(False)
    
    df_cargoflat = df[df['is_deleted'] == False]
    df_cargoflat = df_cargoflat.drop_duplicates()
    df_cargoflat = df_cargoflat[['port_origin', 'port_destination', 'type', 'fare']]
    df_cargoflat = df_cargoflat.rename(columns={'port_origin' : 'id_origin',
                                                'port_destination' : 'id_destination'})
    return df_cargoflat

# %%
def retrieve_rlsroute():
    sql = 'select * FROM rms_pelni.rls_route'
    df = pd.read_sql(sql, engine)

    df_rlsroute = df[df['is_deleted'] == False]
    df_rlsroute = df_rlsroute[df_rlsroute['status'] == 'ACTIVE']
    df_rlsroute = df_rlsroute.drop_duplicates()
    df_rlsroute = df_rlsroute[['id_rls_route', 'avg_factor', 'avg_onboard', 'commision', 'tot_cost', 'tot_distance', 
                               'tot_revenue', 'tot_total', 'type_season', 'id_est_route', 'id_ship']]
    df_rlsroute = df_rlsroute.rename(columns={'id_rls_route' : 'id', 'type_season' : 'season', 'id_est_route' : 'idest', 
                                              'id_ship' : 'idship'})
    return df_rlsroute

# %%
def retrieve_rlsroutedetail():
    sql = 'select * FROM rms_pelni.rls_route_detail '
    df = pd.read_sql(sql, engine)

    df_rlsroutedetail = df[df['is_deleted'] == False]
    df_rlsroutedetail = df_rlsroutedetail.drop_duplicates()
    df_rlsroutedetail = df_rlsroutedetail[['id_rls_route_detail', 'factor', 'rls_down', 'rls_onboard', 'rls_revenue', 
                                           'rls_total', 'rls_up', 'ruas', 'type', 'id_port_origin', 'id_rls_route']]
    df_rlsroutedetail = df_rlsroutedetail.rename(columns={'id_rls_route_detail' : 'id', 'rls_down' : 'down', 
                                                          'rls_onboard' : 'onboard', 'rls_revenue' : 'revenue', 
                                                          'rls_total' : 'total', 'rls_up' : 'up', 
                                                          'id_port_origin' : 'idport', 'id_rls_route' : 'idrls'})
    return df_rlsroutedetail

# %%
def retrieve_routecost():
    sql = 'select * FROM rms_pelni.rls_route_cost'
    df = pd.read_sql(sql, engine)

    df_routecost = df[df['is_deleted'] == False]
    df_routecost = df_routecost.drop_duplicates()
    df_routecost = df_routecost[['id_rls_cost', 'berthing_time', 'npax', 'sailing_time', 'subtotal_cost']]
    df_routecost = df_routecost.rename(columns={'id_rls_cost' : 'id'})
    return df_routecost

# %%
def retrieve_tide():
    sql = 'select * FROM rms_pelni.master_tide'
    df = pd.read_sql(sql, engine)

    df_tide = df[df['is_deleted'] == False]
    df_tide = df_tide.drop_duplicates()
    df_tide = df_tide[['id_tide', 'tide', 'start_date_time', 'end_date_time', 'id_port']]
    df_tide = df_tide.rename(columns={'start_date_time' : 'start', 'end_date_time' : 'end'})
    return df_tide

# %%
def retrieve_revenue_period(typerev, date_start, date_end):
    sql =  f"select * from rms_pelni.hist_pax_revenue hpr where type_rev = {typerev} and departure_date between '{date_start}' and '{date_end}'"
    df = pd.read_sql(sql, engine)

    #df_revenue = df[df['type_rev'] == type_rev]
    df_revenue = df[df['is_deleted'] == False]
    df_revenue = df_revenue.drop_duplicates()
    df_revenue = df_revenue[['origin_port', 'destination_port', 'departure_date',
                             'departure', 'revenue', 'total', 'type']]
    df_revenue = df_revenue.rename(columns={'origin_port' : 'origin',
                                            'destination_port' : 'destination',
                                            'departure_date' : 'depdate',
                                            'departure' : 'deptime'})
    return df_revenue

# %%
def merge_hist_pax(start,end):
    hist_rev1 = retrieve_revenue_period(1,start,end)
    hist_rev2 = retrieve_revenue_period(2,start,end)
    merged_df = pd.merge(hist_rev1, hist_rev2, on=[
                        'origin', 'destination', 'type'], how='outer', indicator=True)
    
    merged_df_right = merged_df[merged_df['_merge']=='right_only']
    merged_df_right = merged_df_right.drop(columns=['depdate_x', 'deptime_x', 'revenue_x', 'total_x','_merge'])
    merged_df_right.rename(columns={
        'depdate_y':'depdate',
        'deptime_y':'deptime',
        'revenue_y':'revenue',
        'total_y':'total'
    }, inplace=True)
   
    merged_df_left = merged_df[merged_df['_merge'] == 'left_only']
    merged_df_left = merged_df_left.drop(columns=['depdate_y', 'deptime_y', 'revenue_y', 'total_y','_merge'])
    merged_df_left.rename(columns={
        'depdate_x': 'depdate',
        'deptime_x': 'deptime',
        'revenue_x': 'revenue',
        'total_x': 'total'
    }, inplace=True)

    merged_hist_pax = pd.merge(merged_df_left, merged_df_right, how='outer')

    return merged_hist_pax

# %%
def cleansing_portdistance(df_port, df_portdistance) :
    #global df_portdistance

    # Add data dummy
    # dum = copy.deepcopy(df_port[df_port['portno'] == 10])
    # dum = dum.reset_index()
    # dum.drop('portno', axis=1, inplace=True)

    # # Update data dummy
    # dum['id'], dum['portcode'], dum['name'], dum['code'], dum['lat'], dum['long'], dum['hightide'], dum['lowtide'], dum['length'] = '111', 'AZZZ', 'DUMMY', '99001', '-6.1312617', '106.6409313', '3.1', '4.1', '99' 
    # df_port = pd.concat([df_port, dum])

    #START function
    portlist = copy.deepcopy(list(df_port.id))
    od_pair = pd.DataFrame((itertools.permutations(portlist, 2)), columns=['id_origin', 'id_destination'])

    # Permutation (id_port)
    df_join = pd.merge(df_portdistance, od_pair, how='right', left_on=('id_origin', 'id_destination'), right_on=('id_origin', 'id_destination'))
    notfound = df_join[df_join['nautical'].isna()]
    # notfound
    df = notfound.drop(columns=['origin','destination'])
    
    df_ori = copy.deepcopy(df_port.loc[:, ['id','lat', 'long']])
    nf = notfound.loc[:, ['id_origin','id_destination']]
    df_des = copy.deepcopy(df_ori)

    # Change name columns
    df_ori = df_ori.rename(columns={'id': 'id_origin', 'lat': 'lat_origin', 'long': 'long_origin'})
    df_des = df_des.rename(columns={'id': 'id_destination', 'lat': 'lat_destination', 'long': 'long_destination'})
    
    # Change Typedate
    nf['id_origin'] = nf['id_origin'].astype(str)
    df_ori['id_origin'] = df_ori['id_origin'].astype(str)

    # Inner Join
    df = pd.merge(nf, 
                df_ori, 
                on ='id_origin', 
                how ='inner')
    
    # Change Typedata
    df['id_destination'] = df['id_destination'].astype(str)
    df_des['id_destination'] = df_des['id_destination'].astype(str)

    # Inner Join
    df = pd.merge(df,
                    df_des,
                    on ='id_destination', 
                    how ='inner')

    # Change Typedata
    df['lat_origin'] = df['lat_origin'].astype(float)
    df['long_origin'] = df['long_origin'].astype(float)
    df['lat_destination'] = df['lat_destination'].astype(float)
    df['long_destination'] = df['long_destination'].astype(float)

    # Calculate from latitude and longtitude data convert to miles (use other library)
    nautical_data = []
    for index, row in df.iterrows():
        ori = (row['lat_origin'],row['long_origin'])
        des = (row['lat_destination'],row['long_destination'])
        rs = gd(ori, des).miles
        rs = round(rs)
        if rs==0:
            rs = 99999
        nautical_data.append(rs)
        
    # Add new columns    
    df['nautical'] = nautical_data
    df['commercial'] = nautical_data
    df['origin'] = 99999
    df['destination'] = 99999
    if len(df)>0:
        df_portdistance = pd.concat([df_portdistance,df[['origin','destination','nautical','commercial','id_origin','id_destination']]])
    
    return df_portdistance

# %%
# def cleaned_port(df_port):
#     port = copy.deepcopy(df_port)
    
#     port['depth'] = port['depth'].astype(str).str.extract('(\d+)')
#     port.loc[((port['depth'] == '') | (port['depth'] == None) | (port['depth'].isna())), 'depth'] = 10
#     port['depth'] = port['depth'].astype(float)

#     port['length'] = port['length'].astype(str).str.extract('(\d+)')
#     port.loc[((port['length'] == '') | (port['length'] == None) | (port['length'].isna())), 'length'] = 200
#     port['length'] = port['length'].astype(float)

#     port['avgberth'] = port['avgberth'].astype(str).str.extract('(\d+)').astype(float)
#     port.loc[((port['avgberth'] == '') | (port['avgberth'] == None) | (port['avgberth'].isna())), 'avgberth'] = 2
    
#     port['lat'] = port['lat'].astype(str).str.extract('(\d+)')
#     port.loc[((port['lat'] == '') | (port['lat'] == None) | (port['lat'].isna())), 'lat'] = 24
#     port['lat'] = port['lat'].astype(float)

#     port['long'] = port['long'].astype(str).str.extract('(\d+)')
#     port.loc[((port['long'] == '') | (port['long'] == None) | (port['long'].isna())), 'long'] = (-96)
#     port['long'] = port['long'].astype(float)
    
#     port['noberth'] = port['noberth'].astype(str).str.extract('(\d+)')
#     port.loc[((port['noberth'] == '') | (port['noberth'] == None) | (port['noberth'].isna())), 'noberth'] = 1
#     port['noberth'] = port['noberth'].astype(int)

#     for i,p in port.iterrows():
#         port.loc[i,'minberthtime'] = str(port.loc[i,'minberthtime'])
#         port.loc[i,'maxberthtime'] = str(port.loc[i,'maxberthtime'])
#     minberthtime = '00:00:00'
#     minberthtime = datetime.strptime(minberthtime, '%H:%M:%S')
#     port.loc[((port['minberthtime'] == 0) | (port['minberthtime'] == 'None') | (port['minberthtime'].isna())), 'minberthtime'] = minberthtime
#     port['minberthtime'] = pd.to_datetime(port['minberthtime'])
#     port['minberthtime'] = port['minberthtime'].dt.time
        
#     maxberthtime = '23:59:59'
#     maxberthtime = datetime.strptime(maxberthtime, '%H:%M:%S')
#     port.loc[((port['maxberthtime'] == 0) | (port['maxberthtime'] == 'None') | (port['maxberthtime'].isna())), 'maxberthtime'] = maxberthtime
#     port['maxberthtime'] = pd.to_datetime(port['maxberthtime'])
#     port['maxberthtime'] = port['maxberthtime'].dt.time

#     return port


