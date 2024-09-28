# %% [markdown]
# General Functions

# %%
import pandas as pd
from random import choices, randint, randrange, random, shuffle
from .data_preparation_k import *
import warnings
import math
import time
import datetime
import copy
from datetime import datetime, timedelta

# %%
def unique(lists):
    unique_list = pd.Series(lists).drop_duplicates().tolist()
    return list(unique_list)

# %%
def calculate_totdistance(id_port, df_portdistance):
    #global df_portdistance
    
    size = len(id_port) - 1
    origin = []
    destination = []

    for i in range(size):
        a = id_port[i]
        b = id_port[(i+1)]
        origin.append(a)
        destination.append(b)
           
    od_pair = pd.DataFrame({'id_origin' : origin, 'id_destination' : destination})

    df_join = pd.merge(df_portdistance, od_pair, how='inner', left_on=('id_origin', 'id_destination'), right_on=('id_origin', 'id_destination'))

    total_nautical = df_join['nautical'].sum()
    return total_nautical

# %%
def commission_days(id_ship, listport, df_ship, df_port, df_portdistance):
    
    df_ship1 = df_ship[df_ship['id'] == id_ship]

    df_port1 = df_port[df_port['id'].isin(listport)]
    berthing_time = df_port1['avgberth'].sum()

    totdistance = calculate_totdistance(listport, df_portdistance)
    sailing_time = totdistance/df_ship1.iloc[0]['speed']
    
    comm_days = round((sailing_time + berthing_time)/24, 0)

    return comm_days

# %%
def calculate_sailingtime(id_ship, id_port1, id_port2, df_ship, df_portdistance):
    #global df_ship, df_port, df_portdistance
    listport = list([id_port1,id_port2])
    totdistance = calculate_totdistance(listport, df_portdistance)
    df_ship1 = df_ship[df_ship['id'] == id_ship]
    
    # df_distance1 = df_distance.fillna(0)
    sailing_time = (totdistance/df_ship1.iloc[0]['speed'])
    
    return sailing_time

# %%
def eligible_berthing(id_ship, id_port, berthingdatetime, all_schedule, df_ship, df_port, df_tide):
    ships = copy.deepcopy(df_ship)
    ports = copy.deepcopy(df_port)
    tides = copy.deepcopy(df_tide)

    berthingtime = pd.Timestamp(berthingdatetime).time()
    ship = ships[ships['id'] == id_ship]
    port = ports[(ports['id'] == id_port)]
    maxnoberth = port['noberth'].iloc[0]
    berth = port['avgberth'].iloc[0]
    deptime = berthingdatetime+pd.Timedelta(hours=berth)

    tide = tides[(tides['id_port'] == id_port) & (tides['start'] <= berthingdatetime) & (tides['end'] >= berthingdatetime)]
    # checking tide
    if len(tide) > 0:
        if ship['draft'].iloc[0] < tide['tide'].iloc[0]:
            eligible_berthingtime = True
        else:
            return False
    else:
        eligible_berthingtime = True
    
    # checking berth time
    if eligible_berthingtime == True:
        if (port['minberthtime'].iloc[0] <= berthingtime) & (port['maxberthtime'].iloc[0] >= berthingtime):
            return check_isberth(id_port, maxnoberth, berthingdatetime, deptime, all_schedule)
        else:
            return False

# %%
def calculate_time(id_ship, id_port, df_ship, df_port, df_portdistance):
    #global df_ship, df_port, df_portdistance
    
    totdistance = calculate_totdistance(id_port, df_portdistance)
    df_ship1 = df_ship[df_ship['id'] == id_ship]
    
    # df_distance1 = df_distance.fillna(0)
    sailing_time = (totdistance/df_ship1.iloc[0]['speed'])/24
    
    df_port1 = df_port[df_port['id'].isin(id_port)]
    berthing_time = df_port1['avgberth'].sum()/24

    comm_days = math.ceil(berthing_time + sailing_time)
    
    data = {'Name' : ['Sailing Time', 'Berthing Time', 'Commission Days'],
            'Amount' : [sailing_time, berthing_time, comm_days]}
    df = pd.DataFrame(data)
    return df

# %%
def calculate_list_schedule(id_ship, listport, datetime_awal, all_schedule, df_ship, df_port, df_portdistance, istide=None, df_tide=None):
    list_schedule = pd.DataFrame(columns=['idship', 'idport', 'ruas', 'arrival_time', 'departure_time'])

    datetime_awal_timestamp = copy.deepcopy(pd.Timestamp(datetime_awal))

    arrival_awal = copy.deepcopy(datetime_awal_timestamp)
    for i in range(len(listport)):
        if i == 0:
            sailing_time = 0
        else:
            sailing_time = round(calculate_sailingtime(id_ship, listport[i-1], listport[i], df_ship, df_portdistance)*60,0)       
        
        arrival_time = arrival_awal + pd.Timedelta(minutes=sailing_time)
        if istide is not None:
            while eligible_berthing(id_ship, listport[i], arrival_time, all_schedule, df_ship, df_port, df_tide)==False:
                arrival_time = arrival_time+pd.Timedelta(minutes=5)

        port = df_port[df_port['id'] == listport[i]]
        berthing = round(float(port['avgberth'].iloc[0])*60,0)
        departure_time = arrival_time + pd.Timedelta(minutes=berthing)

        arrival_awal = copy.deepcopy(departure_time)
        add_schedule = pd.DataFrame({'idship' : id_ship, 'idport' : listport[i], 'ruas' : i, 'arrival_time' : arrival_time, 
                                    'departure_time' : departure_time}, index=[i])
        list_schedule = pd.concat([list_schedule, add_schedule])
    return list_schedule

# %%
def calculate_pax_prediction_with_dot(portlist,df_r, dot_start=None, dot_end=None):
    # df_revenue['depdate'] = pd.to_datetime(df_revenue['depdate'], format='%Y-%m-%d')
    if dot_end is not None:
        date_start = pd.to_datetime(dot_start, format='%Y-%m-%d')
        date_end = pd.to_datetime(dot_end, format='%Y-%m-%d')
        
        revenue = copy.deepcopy(df_r)#[df_revenue['type'] == 'PASSENGER'])
        revenue = revenue[(revenue['depdate'] >= date_start) & (revenue['depdate'] <= date_end)]
    else:
        revenue = df_r
    origin = []
    destination = []
    size = len(portlist) - 1

    for i in range(size):
        a = portlist[i]
        for j in range(size - i):
            idx = i + j + 1
            b = portlist[idx]
            origin.append(a)
            destination.append(b)
    
    if len(revenue)==0:
        df_join = pd.DataFrame({'origin' : origin, 'destination' : destination, 'depdate': '1990-01-01', 'deptime':'1990-01-01',
                            'revenue':0, 'total':0, 'type':'PASSENGER'})
    else:
        od_pair = pd.DataFrame({'origin' : origin, 'destination' : destination})
        df_joinpax = pd.merge(revenue[revenue.type=='PASSENGER'], od_pair, how='right', left_on=('origin', 'destination'), right_on=('origin', 'destination'))
        df_joinpax.loc[df_joinpax['type'].isnull(),'type'] ='PASSENGER'
        df_joinpax.loc[df_joinpax['total'].isnull(),'total'] =0
        df_joincargo = pd.merge(revenue[revenue.type!='PASSENGER'], od_pair, how='inner', left_on=('origin', 'destination'), right_on=('origin', 'destination'))
        df_join = pd.concat([df_joinpax,df_joincargo], ignore_index=True)
                
        # od_pair = pd.DataFrame({'origin' : origin, 'destination' : destination})
        # df_join = pd.merge(revenue, od_pair, how='right', left_on=('origin', 'destination'), right_on=('origin', 'destination'))
        # df_join.loc[df_join['type'].isnull(),'type'] ='PASSENGER'
        # df_join.loc[df_join['total'].isnull(),'total'] =0
        # df_join.fillna(0)
        # df_join.loc[df_join.type==0,'type']='PASSENGER'
        # if 'PASSENGER' not in list(df_join['type'].unique()):
        #     df_pax = copy.deepcopy(df_join)
        #     df_pax.loc[:,'type']='PASSENGER'
        #     df_pax.loc[:,'total']=0
        #     df_join = pd.concat([df_join,df_pax], ignore_index=True)
        
    return df_join

# %%
def covered_demand_with_dot(covered_demand1):
    # covered_demand1 = calculate_pax_prediction_with_dot(portlist, df_r,dot_start, dot_end)
    if len(covered_demand1)>0:    
        # movements_list = []
        # for index, row in covered_demand1.iterrows():
        #     source_port = str(row['origin'])
        #     dest_port = str(row['destination'])
        #     type = str(row['type'])
        #     # cargo_type = str(row.iloc[2]).replace("Name: 0, dtype: object", "").strip()
        #     total = row['total']
        #     movement_data = {
        #         'origin': source_port,
        #         'destination': dest_port,
        #         'type': type,
        #         'total': round(total)
        #     }
        #     movements_list.append(movement_data)
        # movement_df = pd.DataFrame(movements_list)
        movement_df = covered_demand1.groupby(['origin','destination','type'], as_index=False)['total'].sum()
    else:
        movement_df = pd.DataFrame(columns=['origin','destination','type','total'])
    return movement_df

# %%
def separate_demand(portlist1, portlist2, cd_all):
    port_ids1 = [list([port,i]) for i, port in enumerate(portlist1)]
    port_ids1 = pd.DataFrame(list(port_ids1), columns=['port','ruas'])
    movement_df1 = pd.merge(cd_all,port_ids1, how='left', left_on=['origin'], right_on=['port']).drop(columns = ['port'])
    movement_df1 = movement_df1.rename(columns={'ruas':'source_id'})

    movement_df1 = pd.merge(movement_df1,port_ids1, how='left', left_on=['destination'], right_on=['port']).drop(columns = ['port'])
    movement_df1 = movement_df1.rename(columns={'ruas':'destination_id'})
    movement_df1 = movement_df1[movement_df1['source_id']<movement_df1['destination_id']]
    
    port_ids2 = [list([port,i]) for i, port in enumerate(portlist2)]
    port_ids2 = pd.DataFrame(list(port_ids2), columns=['port','ruas'])
    movement_df2 = pd.merge(cd_all,port_ids2, how='left', left_on=['origin'], right_on=['port']).drop(columns = ['port'])
    movement_df2 = movement_df2.rename(columns={'ruas':'source_id'})

    movement_df2 = pd.merge(movement_df2,port_ids2, how='left', left_on=['destination'], right_on=['port']).drop(columns = ['port'])
    movement_df2 = movement_df2.rename(columns={'ruas':'destination_id'})
    movement_df2 = movement_df2[movement_df2['source_id']<movement_df2['destination_id']]
    
    return movement_df1, movement_df2

# %%
def count_times_between(a_datetime, b_datetime, x):
    # Convert string timestamps to datetime objects
    format_str = '%Y-%m-%d %H:%M:%S'
    count = 0

    a_date = datetime.strftime(a_datetime, '%Y-%m-%d')
    strtime=str(a_date)+' '+x
    x_datetime = datetime.strptime(strtime, format_str)
    curr_time = x_datetime
    while curr_time <= b_datetime:
        if curr_time>=a_datetime:
            count += 1
        curr_time += timedelta(days=1)
    return count

# %%
def revise_coverage(output, capacity):
    for i, row in output.iterrows():
        if row.factor>1:
            ruas = row.ruas
            before = (output.ruas<ruas)&(output.type==row.type)
            rowb = output[before]
            if len(rowb)>0:
                rowb = rowb[rowb.ruas==max(rowb.ruas)]
                rowb = rowb.iloc[0]
                new_in = row['up']
                new_out = min(row.down,rowb.onboard)
                            
                new_onboard = min((rowb['onboard']-new_out+row['up']),capacity[row.type])
                new_in = min((new_onboard-rowb.onboard+new_out), capacity[row.type])
            else:
                new_onboard = capacity[row.type]
                new_in = capacity[row.type]
                new_out = row.down
        
            this = (output.ruas==row.ruas)&(output.type==row.type)&(output.port==row.port)
            output.loc[this,'up']=new_in
            output.loc[this,'down']=new_out
            output.loc[this,'onboard']=new_onboard
            output.loc[this,'factor']=new_onboard/capacity[row.type]

    return output      

# %%
def ship_capacity(idship, df_ship):
    ship = df_ship[df_ship['id']==idship]
        
    if ship is None:
        ship = {}

    ship_capacity = {
        'PASSENGER': float(ship['capacitypax'].iloc[0]),
        'GENERAL_CARGO': float(ship['capacitycargo'].iloc[0]),
        'DRY_CONTAINER': float(ship['capacitydry'].iloc[0]),
        'REEFER_CONTAINER': float(ship['capacityreefer'].iloc[0]),
        'TRUK': float(ship['capacitytruck'].iloc[0]),
        'MOBIL': float(ship['capacitycar'].iloc[0]),
        'MOTOR': float(ship['capacitymotor'].iloc[0]),
        'REDPACK': float(ship['capacityredpack'].iloc[0])
    }

    # Replace values with 0 if they are None or NaN
    for key, value in ship_capacity.items():
        if value is None or math.isnan(value):
            ship_capacity[key] = 0

    return ship_capacity

# %%
def separate_portlist(portlist):
    end1 = math.ceil(len(portlist)/2)
    start2 = math.floor(len(portlist)/2)
    portlist1 = portlist[0:end1]
    portlist2 = portlist[start2:]
    x1 = copy.deepcopy(list(reversed(portlist2)))
    x2 = copy.deepcopy(portlist1)
    if x1!=x2:  
        for i in range(1,len(portlist)):
            if portlist[i] in portlist[0:(i-1)]:
                break
        portlist1 = portlist[0:i]
        portlist2 = portlist[(i-1):]

    return portlist1,portlist2

# %%
def cal_coverage_with_dot(idShip, portList, prediction, df_ship):
    capacity = ship_capacity(idShip, df_ship)
    output = pd.DataFrame(columns=['port','ruas','type','up','down','onboard','factor'])
    
    output_rev = pd.DataFrame(columns=['port','ruas','type','up','down','onboard','factor'])
    movement_df = covered_demand_with_dot(prediction)
    if len(movement_df)>0:
        # Create initial IDs for source and destination ports
        port_ids = [list([port,i]) for i, port in enumerate(portList)]
        port_ids = pd.DataFrame(list(port_ids), columns=['port','ruas'])
        movement_df = pd.merge(movement_df,port_ids, how='left', left_on=['origin'], right_on=['port']).drop(columns = ['port'])
        movement_df = movement_df.rename(columns={'ruas':'source_id'})

        movement_df = pd.merge(movement_df,port_ids, how='left', left_on=['destination'], right_on=['port']).drop(columns = ['port'])
        movement_df = movement_df.rename(columns={'ruas':'destination_id'})
        
        cargo_types = ['DRY_CONTAINER', 'REEFER_CONTAINER', 'TRUK', 'MOBIL', 'MOTOR', 'REDPACK', 'GENERAL_CARGO', 'PASSENGER']
        for no, port_name in port_ids.iterrows():
            insum = movement_df[movement_df['source_id'] == port_name['ruas']][['type','total']]
            in_sum = insum.groupby(['type'], as_index=False)['total'].sum()
            in_sum = in_sum.rename(columns={'total':'up'})

            outsum = movement_df[movement_df['destination_id'] == port_name['ruas']][['type','total']]
            out_sum = outsum.groupby(['type'], as_index=False)['total'].sum()
            out_sum = out_sum.rename(columns={'total':'down'})

            onboard_mask = ((movement_df['source_id'] <= port_name['ruas']) & (movement_df['destination_id'] > port_name['ruas']))
            onboard = movement_df.loc[onboard_mask, ['type','total']]
            on_board = onboard.groupby(['type'], as_index=False)['total'].sum()
            on_board = on_board.rename(columns={'total':'onboard'})

            a = pd.merge(in_sum,out_sum, how='outer', on='type')
            b = pd.merge(a,on_board, how='outer', on='type')
            
            if len(b)>0:
                for cargo_type in cargo_types: 
                    if capacity[cargo_type]==0:
                        b = b.drop(list(b.loc[b['type'] == cargo_type].index))
                    else:
                        b.loc[b['type'] == cargo_type, 'factor'] = b.loc[b['type'] == cargo_type, 'onboard'] / capacity[cargo_type]
                if len(b)>0:
                    b.loc[:,'port'] = port_name['port']
                    b.loc[:,'ruas'] = port_name['ruas']
                
            if not b.empty:
                output = pd.concat([output,b], ignore_index=True)
                
        output_to_rev = copy.deepcopy(output)
        output_rev = revise_coverage(output_to_rev, capacity)
        return output_rev.fillna(0)
    else:
        return []

# %%
# def cal_coverage(idShip, portList, movement_df, df_ship):
    
#     capacity = ship_capacity(idShip, df_ship)
#     output = pd.DataFrame(columns=['port','ruas','type','up','down','onboard','factor'])
        
#     if len(movement_df)>0:
#         # Create initial IDs for source and destination ports
#         port_ids = [list([port,i]) for i, port in enumerate(portList)]
#         port_ids = pd.DataFrame(list(port_ids), columns=['port','ruas'])

#         cargo_types = ['DRY_CONTAINER', 'REEFER_CONTAINER', 'TRUK', 'MOBIL', 'MOTOR', 'REDPACK', 'GENERAL_CARGO', 'PASSENGER']
#         for no, port_name in port_ids.iterrows():
#             insum = movement_df[movement_df['source_id'] == port_name['ruas']][['type','total']]
#             in_sum = insum.groupby(['type'], as_index=False)['total'].sum()
#             in_sum = in_sum.rename(columns={'total':'up'})

#             outsum = movement_df[movement_df['destination_id'] == port_name['ruas']][['type','total']]
#             out_sum = outsum.groupby(['type'], as_index=False)['total'].sum()
#             out_sum = out_sum.rename(columns={'total':'down'})

#             onboard_mask = ((movement_df['source_id'] <= port_name['ruas']) & (movement_df['destination_id'] > port_name['ruas']))
#             onboard = movement_df.loc[onboard_mask, ['type','total']]
#             on_board = onboard.groupby(['type'], as_index=False)['total'].sum()
#             on_board = on_board.rename(columns={'total':'onboard'})

#             a = pd.merge(in_sum,out_sum, how='outer', on='type')
#             b = pd.merge(a,on_board, how='outer', on='type')
            
#             if len(b)>0:
#                 for cargo_type in cargo_types: 
#                     if capacity[cargo_type]==0:
#                         b = b.drop(list(b.loc[b['type'] == cargo_type].index))
#                     else:
#                         b.loc[b['type'] == cargo_type, 'factor'] = b.loc[b['type'] == cargo_type, 'onboard'] / capacity[cargo_type]
#                 if len(b)>0:
#                     b.loc[:,'port'] = port_name['port']
#                     b.loc[:,'ruas'] = port_name['ruas']
                
#             if not b.empty:
#                 output = pd.concat([output,b], ignore_index=True)

#     return output.fillna(0)
def cal_coverage(idShip, portList, movement_df, df_ship):
    capacity = ship_capacity(idShip, df_ship)
    output = pd.DataFrame(columns=['port','ruas','type','up','down','onboard','factor'])
    
    if len(movement_df)>0:
        # Create initial IDs for source and destination ports
        port_ids = [list([port,i]) for i, port in enumerate(portList)]
        port_ids = pd.DataFrame(list(port_ids), columns=['port','ruas'])

        cargo_types = ['DRY_CONTAINER', 'REEFER_CONTAINER', 'TRUK', 'MOBIL', 'MOTOR', 'REDPACK', 'GENERAL_CARGO', 'PASSENGER']
        for no, port_name in port_ids.iterrows():
            insum = movement_df[movement_df['source_id'] == port_name['ruas']][['type','total']]
            in_sum = insum.groupby(['type'], as_index=False)['total'].sum()
            in_sum = in_sum.rename(columns={'total':'up'})

            outsum = movement_df[movement_df['destination_id'] == port_name['ruas']][['type','total']]
            out_sum = outsum.groupby(['type'], as_index=False)['total'].sum()
            out_sum = out_sum.rename(columns={'total':'down'})

            onboard_mask = ((movement_df['source_id'] <= port_name['ruas']) & (movement_df['destination_id'] > port_name['ruas']))
            onboard = movement_df.loc[onboard_mask, ['type','total']]
            on_board = onboard.groupby(['type'], as_index=False)['total'].sum()
            on_board = on_board.rename(columns={'total':'onboard'})

            a = pd.merge(in_sum,out_sum, how='outer', on='type')
            b = pd.merge(a,on_board, how='outer', on='type')
            b = b.fillna(0)
            if len(b)>0:
                for cargo_type in cargo_types: 
                    if capacity[cargo_type]==0:
                        b = b.drop(list(b.loc[b['type'] == cargo_type].index))
                        if len(movement_df.loc[(movement_df["type"]==cargo_type)])>0:
                            movement_df.loc[(movement_df["type"]==cargo_type),'total']=0
                        if cargo_type=='PASSENGER':
                            addb = pd.DataFrame({'type':cargo_type,'up':0,'down':0,'onboard':0,'factor':0}, index=[len(b)+1])
                            b = pd.concat([b,addb])
                    elif len(b[b['type'] == cargo_type])>0:                        
                        lf=b.loc[b['type'] == cargo_type, 'onboard'].iloc[0] / capacity[cargo_type]
                        if lf>1:
                            lf=1
                            if port_name['ruas']==0:
                                up=capacity[cargo_type]
                                onb=capacity[cargo_type]
                                down=0
                            else:
                                before=output[(output.ruas==port_name['ruas']-1)&(output.type==cargo_type)]
                                down=b.loc[b['type'] == cargo_type, 'down'].iloc[0]
                                if len(before)>0:
                                    b_onb=before['onboard'].iloc[0]
                                    if down>b_onb:
                                        down = b_onb
                                else:
                                    b_onb=0
                                up=b.loc[b['type'] == cargo_type, 'up'].iloc[0]
                                onb=b.loc[b['type'] == cargo_type, 'onboard'].iloc[0]
                                newup=b_onb-down+up
                                if newup>capacity[cargo_type]:
                                    up=capacity[cargo_type]-(b_onb-down)
                                    onb=capacity[cargo_type]
                            maxruas = max(port_ids['ruas'])
                            a_ruas=port_name['ruas']+1
                            sisa = copy.deepcopy(up)
                            while a_ruas<=maxruas:
                                mdf = movement_df[(movement_df["source_id"]==port_name['ruas'])&(movement_df["destination_id"]==a_ruas)&(movement_df["type"]==cargo_type)]
                                if len(mdf)>0:
                                    isi = copy.deepcopy(mdf['total'].iloc[0])
                                    isi = min(sisa,isi)
                                    movement_df.loc[(movement_df["source_id"]==port_name['ruas'])&(movement_df["destination_id"]==a_ruas)&(movement_df["type"]==cargo_type),'total']=isi
                                    sisa = sisa-isi
                                a_ruas = a_ruas+1
                            b.loc[b['type'] == cargo_type, 'up'] = up
                            b.loc[b['type'] == cargo_type, 'down'] = down
                            b.loc[b['type'] == cargo_type, 'onboard'] = onb   
                        b.loc[b['type'] == cargo_type, 'factor'] = lf
                    else:
                        if cargo_type=='PASSENGER':
                            addb = pd.DataFrame({'type':cargo_type,'up':0,'down':0,'onboard':0,'factor':0}, index=[len(b)+1])
                            b = pd.concat([b,addb])
                if len(b)>0:
                    b.loc[:,'port'] = port_name['port']
                    b.loc[:,'ruas'] = port_name['ruas']
                
            if not b.empty:
                output = pd.concat([output,b], ignore_index=True)

    return output.fillna(0),movement_df

# %%
def cal_factor(idShip, portList, cd_all, df_ship):
    
    if (len(unique(portList))!=len(portList)) and (len(portList)>2):
        portlist1,portlist2 = separate_portlist(portList)
        movement_df1, movement_df2 = separate_demand(portlist1, portlist2, cd_all)
        factor1,movement1 = cal_coverage(idShip, portlist1, movement_df1, df_ship)
        factor2,movement2 = cal_coverage(idShip, portlist2, movement_df2, df_ship)
        if len(movement_df1)==0:
            return pd.DataFrame(), pd.DataFrame()
        maxruas = copy.deepcopy(max(factor1['ruas']))
        minruas = 0
        keep1 = copy.deepcopy(factor1[factor1['ruas']==maxruas])
        factor1 = factor1[factor1['ruas']!=maxruas]
        keep2 = copy.deepcopy(factor2[factor2['ruas']==minruas])
        factor2 = factor2[factor2['ruas']!=minruas]
        factor2.loc[:,'ruas']=factor2.loc[:,'ruas']+maxruas
        middleport = pd.merge(keep1[['port','ruas','type','down']],keep2[['port','type','up','onboard','factor']],
                            how='inner',left_on=('port', 'type'), right_on=('port', 'type'))
        factor = pd.concat([factor1,middleport,factor2], ignore_index=True)
        movement = pd.concat([movement1,movement2], ignore_index=True)
    else:
        port_ids = [list([port,i]) for i, port in enumerate(portList)]
        port_ids = pd.DataFrame(list(port_ids), columns=['port','ruas'])
        movement_df = pd.merge(cd_all,port_ids, how='left', left_on=['origin'], right_on=['port']).drop(columns = ['port'])
        movement_df = movement_df.rename(columns={'ruas':'source_id'})

        movement_df = pd.merge(movement_df,port_ids, how='left', left_on=['destination'], right_on=['port']).drop(columns = ['port'])
        movement_df = movement_df.rename(columns={'ruas':'destination_id'})
        movement_df = movement_df[movement_df['source_id']<movement_df['destination_id']]
        factor,movement = cal_coverage(idShip, portList, movement_df, df_ship)
    
    movement = movement[['origin', 'destination', 'type', 'total']]
    return factor,movement

# %%
def calculate_cost_with_time(id_ship, portlist, all_schedule, df_ship, df_port, df_portdistance, df_rulecost, coverage, dot_start):
    
    list_schedule = calculate_list_schedule(id_ship, portlist, dot_start, all_schedule, df_ship, df_port, df_portdistance)
    size = len(list_schedule) - 1 

    rulecost = copy.deepcopy(df_rulecost)
    rulecost = rulecost[rulecost['localtime'].notnull()]
    
    # coverage = cal_coverage_with_dot(id_ship, portlist, prediction, df_ship)
    # coverage,m = cal_factor(id_ship, portlist, prediction, df_ship)
    coverage = coverage[(coverage['type']=='PASSENGER')]

    list_of_cost = pd.DataFrame(columns=['idrulecost', 'idship', 'port_origin', 'port_destination', 'start', 'end', 
                                         'cost_name', 'basic_cost', 'days', 'pax', 'ruas', 'total_cost'])
    
    for i in range(size): 
        next_list = i + 1
        ship_on_list = list_schedule.loc[list_schedule.ruas==i, 'idship'].iloc[0]
        port_on_list = list_schedule.loc[list_schedule.ruas==i, 'idport'].iloc[0]
        next_port = list_schedule.loc[list_schedule.ruas==next_list, 'idport'].iloc[0]
        origin_arrive = pd.to_datetime(list_schedule.loc[list_schedule.ruas==i, 'arrival_time'].iloc[0])
        destination_arrive = pd.to_datetime(list_schedule.loc[list_schedule.ruas==next_list, 'arrival_time'].iloc[0])

        for j in rulecost.index:
            idrulecost = rulecost.loc[j, 'idrulecost']
            ship_on_rule = rulecost.loc[j, 'idship']
            port_on_rule = rulecost.loc[j, 'idport']
            cost_name = rulecost.loc[j, 'name']
            basic_cost = rulecost.loc[j, 'cost']
            rulecost_localtime =str(rulecost.loc[j, 'localtime'])
            pax_validate = rulecost.loc[j, 'ispax']
            count = count_times_between(origin_arrive, destination_arrive, rulecost_localtime)
            
            # validate localtime
            if count > 0:
                charged_by_localtime = True
            else:
                charged_by_localtime = False

            # validate idship
            if (ship_on_rule == ship_on_list) | (ship_on_rule == None):
                charged_by_ship = True
            else:
                charged_by_ship = False

            # validate idport
            if (port_on_rule == port_on_list) | (port_on_rule == None):
                charged_by_port = True
            else:
                charged_by_port = False
            
            # validate pax
            if pax_validate == True:
                charged_by_pax = True
            else:
                charged_by_pax =  False

            # cost calculation
            if charged_by_localtime == True & charged_by_ship == True & charged_by_port == True:
                if charged_by_pax == True:
                    if len(coverage) > 0:
                        covruas = coverage.loc[coverage['ruas'] == i]
                        if len(covruas)>0:
                            pax = round(covruas['onboard'].iloc[0])
                        else:
                            pax = 0
                    else:
                        pax = 0
                    total_cost = basic_cost * count * pax
                    add_cost = pd.DataFrame({'idrulecost':idrulecost,'idship' : ship_on_list, 'port_origin' : port_on_list, 'port_destination' : next_port,
                                            'start' : origin_arrive, 'end' : destination_arrive, 'cost_name' : cost_name, 
                                            'basic_cost' : basic_cost, 'days' : count, 'pax' : pax, 'ruas':i,'total_cost' : total_cost}, index=[i])
                else:
                    total_cost = basic_cost * count    
                    add_cost = pd.DataFrame({'idrulecost': idrulecost,'idship': ship_on_list, 'port_origin': port_on_list, 'port_destination': next_port,
                                            'start': origin_arrive, 'end': destination_arrive, 'cost_name': cost_name, 
                                            'basic_cost': basic_cost, 'days': count, 'pax': None, 'ruas': i, 'total_cost': total_cost}, index=[i])
                    
                list_of_cost = pd.concat([list_of_cost, add_cost])
    
    list_of_cost = list_of_cost.reset_index(drop=True)
    return list_of_cost

# %%
def decide_time(id_ship, listport, start_route, end_route, all_schedule, df_ship, df_port, df_portdistance, df_rulecost,dfr):
    # dfr = copy.deepcopy(df_revenue[df_revenue['origin'].isin(listport) & df_revenue['destination'].isin(listport)])
    chosen_time = []
    if len(dfr)>0:
        aa = copy.deepcopy(dfr[(dfr['type']=='PASSENGER') & (pd.to_datetime(dfr['depdate']).between(start_route, end_route))])
        if len(aa)==0:
            aa1 = copy.deepcopy(dfr[(dfr['type']=='PASSENGER') & (pd.to_datetime(dfr['depdate'])>=end_route)])
            if len(aa1)>0:
                fixdate = min(list(pd.to_datetime(aa1['depdate'])))
                aa = aa1[(pd.to_datetime(aa1['depdate'])==fixdate)]
        
        prediction = calculate_pax_prediction_with_dot(listport, aa)
        cd_all = covered_demand_with_dot(prediction)
        coverage,m = cal_factor(id_ship, listport, cd_all, df_ship)
        
        mincost = float('inf')
        for i in range(2,26):
            datetime_awal = start_route+pd.Timedelta(hours=i)
            # dot_end = copy.deepcopy(datetime_awal+pd.Timedelta(days=commdays))
            cost_time = calculate_cost_with_time(id_ship, listport, all_schedule, df_ship, df_port, df_portdistance, df_rulecost, coverage, datetime_awal)
            total = cost_time['total_cost'].sum()
            if total<=mincost:
                mincost = copy.deepcopy(total)
                chosen_time = copy.deepcopy(pd.Timestamp(datetime_awal))

    return chosen_time

# %%
def calculate_routecost_with_time(id_ship, portlist, all_schedule, df_rulecost, df_ship, df_port, df_portdistance, coverage, dot_start):
    
    rulecosts = copy.deepcopy(df_rulecost[df_rulecost['localtime'].isnull()])
    rulecosts['ispax'] = rulecosts['ispax'].fillna(False)
    rulecosts['issailing'] = rulecosts['issailing'].fillna(False)
    rulecosts['isberthing'] = rulecosts['isberthing'].fillna(False)
    rulecosts['perday'] = rulecosts['perday'].fillna(np.nan).replace([np.nan], [None])
    
    costdetail = pd.DataFrame(columns=['idrulecost', 'pax', 'day', 'cost'])
    
    for id in rulecosts.index:
        ispax = rulecosts['ispax'][id]
        issail = rulecosts['issailing'][id]
        isberth = rulecosts['isberthing'][id]
        costrule = rulecosts['cost'][id]
        idrulecost = rulecosts['idrulecost'][id]
        ship_rule = rulecosts['idship'][id]
        port_rule = rulecosts['idport'][id]
        perday = rulecosts['perday'][id]
        
        if (ispax == False) & (issail == False) & (isberth == False):
            comm_days = commission_days(id_ship, portlist, df_ship, df_port, df_portdistance)

            if perday == None:
                expense_perday = 1
                fillday = 0
            else:
                expense_perday = perday
                fillday = comm_days
            
            if (ship_rule == None) & (port_rule == None):
                cost = costrule * comm_days * expense_perday
                add_cost = pd.DataFrame({'idrulecost' : idrulecost, 'cost' : cost, 'day':fillday}, index=[id])
                if not add_cost.empty:
                    costdetail = pd.concat([costdetail, add_cost])
            else: 
                if (ship_rule == None):
                    if (port_rule != None):
                        for i in portlist:
                            if (port_rule == i):
                                cost = costrule * comm_days * expense_perday
                                add_cost = pd.DataFrame({'idrulecost' : idrulecost, 'cost' : cost, 'day':fillday}, index=[id])
                                if not add_cost.empty:
                                    costdetail = pd.concat([costdetail, add_cost])
                else:
                    if (ship_rule == id_ship):                 
                        cost = costrule * comm_days * expense_perday
                        add_cost = pd.DataFrame({'idrulecost' : idrulecost, 'cost' : cost, 'day':fillday}, index=[id])
                        if not add_cost.empty:
                            costdetail = pd.concat([costdetail, add_cost])
                
        else:
            timecal = pd.DataFrame(calculate_time(id_ship, portlist, df_ship, df_port, df_portdistance))
            timecal.set_index('Name', inplace=True)
            sailing_time = timecal.loc['Sailing Time']['Amount']
            berthing_time = timecal.loc['Berthing Time']['Amount']

            if isberth == True:
                if berthing_time != 0:
                    cost = berthing_time * perday* costrule
                    
                    add_cost = pd.DataFrame({'idrulecost' : idrulecost, 'cost' : cost, 'day' : berthing_time}, index=[id])
                    if not add_cost.empty:
                        costdetail = pd.concat([costdetail, add_cost])
                else:
                    continue

            elif issail == True:
                if sailing_time != 0:
                    cost = sailing_time * perday * costrule

                    add_cost = pd.DataFrame({'idrulecost' : idrulecost, 'cost' : cost, 'day': sailing_time}, index=[id])
                    if not add_cost.empty:
                        costdetail = pd.concat([costdetail, add_cost])
                else:
                    continue

            elif ispax == True:
                if len(portlist) <= 1:
                    continue
                else:
                    # prediction = pd.DataFrame(calculate_pax_prediction_with_dot(portlist, df_r, dot_start, dot_end))
                    paxcov = coverage[coverage['type'] == 'PASSENGER']
                    comm_days = commission_days(id_ship, portlist, df_ship, df_port, df_portdistance)

                    if len(paxcov) > 0:    
                        npax = round(paxcov['onboard'].sum())
                    else:
                        npax = 0
                    
                    if perday == None:
                        expense_perday = 1
                        fillday = 0
                    else:
                        expense_perday = perday
                        fillday = comm_days

                    cost = npax * expense_perday * comm_days * df_rulecost['cost']

                    add_cost = pd.DataFrame({'idrulecost' : idrulecost, 'pax' : npax, 'cost' : cost, 'day':fillday}, index=[id])
                    if not add_cost.empty:
                        costdetail = pd.concat([costdetail, add_cost])

    costtime = calculate_cost_with_time(id_ship, portlist, all_schedule, df_ship, df_port, df_portdistance, df_rulecost, coverage, dot_start)
    costtime = costtime.drop(columns=['idship', 'port_origin', 'port_destination', 'start', 'end', 'cost_name', 'basic_cost'])
    costtime = costtime.rename(columns={'total_cost' : 'cost', 'days' : 'day'})
    costdetail = pd.concat([costdetail, costtime])

    return costdetail

# %%
def calculate_distanceperport(id_port, df_portdistance): 
    origin = []
    destination = []
    size = len(id_port) - 1

    for i in range(size):
        a = id_port[i]
        for j in range (size - i):
            idx = i + j + 1
            b = id_port[idx]
            origin.append(a)
            destination.append(b)

    od_pair = pd.DataFrame({'id_origin' : origin, 'id_destination' : destination})
    #od_pair = pd.DataFrame((itertools.permutations(id_port, 2)), columns=['id_origin', 'id_destination'])
    
    df_join = pd.merge(df_portdistance, od_pair, how='inner', left_on=('id_origin', 'id_destination'), right_on=('id_origin', 'id_destination')).drop(columns = ['commercial'])
    return df_join

# %%
def calculate_paxprice(distance, dot, df_priceconfig, df_lowpeak, df_basefare, df_adjustment):
    #global df_priceconfig, df_lowpeak, df_basefare, df_adjustment
    
    df_priceconfig1 = df_priceconfig[(df_priceconfig['mindistance'] <= distance) & (df_priceconfig['maxdistance'] >= distance)]

    dot_date = pd.Timestamp(dot)
    
    df_lowpeak1 = df_lowpeak[(pd.to_datetime(df_lowpeak['startdate']) <= dot_date) & (pd.to_datetime(df_lowpeak['enddate']) >= dot_date)]

    #dot_datetime = datetime.datetime.strptime(dot, '%Y-%m-%d')
    df_basefare1 = df_basefare[(df_basefare['startdate'] <= dot_date) & (df_basefare['enddate'] >= dot_date)]
    df_basefare1 = df_basefare1[df_basefare1['typefare'] == 'PASSENGER']
    if len(df_lowpeak1)>0:
        typeseason = df_lowpeak1.iloc[0]['type']
    else:
        typeseason = 'LOW'
    
    df_basefare1 = df_basefare1[df_basefare1['type'] == typeseason]

    if df_priceconfig1.iloc[0]['mindistance'] == 0:
        koefjarak = df_priceconfig1['distancecoeff']
    elif df_priceconfig1.iloc[0]['mindistance'] !=0:
        koefjarak = df_priceconfig1.iloc[0]['distancecoeff'] + ((distance - df_priceconfig1.iloc[0]['mindistance'] - 1) * df_priceconfig1.iloc[0]['coeff'])

    rp = round(koefjarak * df_basefare1.iloc[0]['basefare'], -3)

    if distance < 1000:
        pnp = rp
    elif distance >= 1000:
        pnp = ((100+df_adjustment)/100) * rp

    pnpmile = round(pnp/distance, 2)
    tarif = round(distance * (df_priceconfig1.iloc[0]['coeff']) * (df_priceconfig1.iloc[0]['pangsa']) * pnpmile, -3)
    tarifakhir = round(0.97 * tarif, -3)

    data = {'Name' : ['Tarif', 'Tarif Akhir'],
            'Amount' : [tarif, tarifakhir]}
    df = pd.DataFrame(data)
    return df

# %%
def calculate_cargoprice(distance, dot, df_pricecargoconfig, df_lowpeak, df_basefare, cargotype):
    df_pricecargoconfig1 = df_pricecargoconfig[(df_pricecargoconfig['mindistance'] <= distance) & (df_pricecargoconfig['maxdistance'] >= distance)]
    df_basefare1 = df_basefare[df_basefare['typefare'] == cargotype]

    dot_date = copy.deepcopy(pd.Timestamp(dot))
    df_lowpeak1 = df_lowpeak[(pd.to_datetime(df_lowpeak['startdate']) <= dot_date) & (pd.to_datetime(df_lowpeak['enddate']) >= dot_date)]
    #dot_datetime = datetime.datetime.strptime(dot, '%Y-%m-%d')
    df_basefare1 = df_basefare1[(df_basefare1['startdate'] <= dot_date) & (df_basefare1['enddate'] >= dot_date)]
    
    if len(df_lowpeak1)>0:
            typeseason = df_lowpeak1.iloc[0]['type']
    else:
        typeseason = 'LOW'
    
    df_basefare1 = df_basefare1[df_basefare1['type'] == typeseason]
                                
    fare = df_basefare1.iloc[0]['basefare']

    if len(df_pricecargoconfig1)>0:
        if df_pricecargoconfig1.iloc[0]['mindistance'] == 0:
            koefjarak = df_pricecargoconfig1['distancecoeff']
        elif df_pricecargoconfig1.iloc[0]['mindistance'] != 0:
            koefjarak = df_pricecargoconfig1.iloc[0]['distancecoeff'] + ((distance - df_pricecargoconfig1.iloc[0]['mindistance'] - 1) * df_pricecargoconfig1.iloc[0]['coeff'])
    else:
        koefjarak = 0
        
    tarif = koefjarak * fare
    return tarif

# %%
def cal_port_distance(ports, df_portdistance):
    port_distance = pd.DataFrame(columns=['port','distance'])
    port = ports[0:-1]
    distance = []

    for i in range(len(port)):
        distance.append(calculate_totdistance(list([ports[i],ports[i+1]]),df_portdistance))

    port_distance['port'] = port
    port_distance['distance'] = distance
    return port_distance

# %%
def calculate_revenue_with_dot(portlist, df_cargoflat, df_pricecargoconfig, df_lowpeak, df_basefare,
                                df_priceconfig, df_adjustment, cal_prediction, df_portdistance, dot_start):
    
    cal_distance = pd.DataFrame(calculate_distanceperport(portlist, df_portdistance))
    cal_distance = cal_distance.drop(columns=['origin', 'destination'])
    cal_distance = cal_distance.rename(columns={'id_origin' : 'origin', 'id_destination' : 'destination'})

    if len(cal_prediction)>0:    
        calculate_revenue1 = pd.DataFrame(columns=['origin', 'destination', 'type','total', 'revenue'])
        distance_prediction = pd.merge(cal_prediction, cal_distance, how='inner',left_on=('origin', 'destination'), right_on=('origin', 'destination'))
        distance_prediction = distance_prediction[distance_prediction['type'].notna()]

        cargoflat = df_cargoflat
        cargoflat.loc[cargoflat['type'] == 'DRY', 'type'] = 'DRY_CONTAINER'
        cargoflat_m = cargoflat.rename(columns={'id_origin' : 'origin', 'id_destination' : 'destination', 'fare' : 'cargoflat_fare'})

        distance_prediction_cargoflat = pd.merge(distance_prediction, cargoflat_m, how='left', left_on=('origin','destination','type'), right_on=('origin','destination','type'))

        for index, row in distance_prediction_cargoflat.iterrows():
            distance = row['nautical']
            origin = row['origin']
            destination = row['destination']
            type = row['type']
            total = round(row['total'])

            if type == 'PASSENGER':
                cal_paxprice = pd.DataFrame(calculate_paxprice(distance, dot_start, df_priceconfig, df_lowpeak, df_basefare, df_adjustment))
                cal_paxprice.set_index('Name', inplace=True)
                price = cal_paxprice['Amount']['Tarif Akhir']
            elif type == 'GENERAL_CARGO':
                cal_cargoprice = calculate_cargoprice(distance, dot_start, df_pricecargoconfig, df_lowpeak, df_basefare, 'CARGO')
                #cal_cargoprice.set_index('Name', inplace=True)
                price = cal_cargoprice
            elif type == 'REDPACK':
                cal_cargoprice = calculate_cargoprice(distance, dot_start, df_pricecargoconfig, df_lowpeak, df_basefare, 'REDPACK')
                #cal_cargoprice.set_index('Name', inplace=True)
                price = cal_cargoprice
            else:
                price = row['cargoflat_fare']
            
            revenue = price * total
            add_revenue = pd.DataFrame({'origin' : origin, 'destination' : destination, 'type' : type, 'total' : total, 'revenue' : revenue}, index=[index])
            if not add_revenue.empty:
                calculate_revenue1 = pd.concat([calculate_revenue1, add_revenue])

        calculate_revenue1['revenue'] = calculate_revenue1['revenue'].fillna(0)

        port_ids = [list([port,i]) for i, port in enumerate(portlist)]
        port_ids = pd.DataFrame(list(port_ids), columns=['port','ruas'])

        calculate_revenue1 = pd.merge(calculate_revenue1,port_ids, how='left', left_on=['origin'], right_on=['port']).drop(columns = ['port'])
        calculate_revenue1 = calculate_revenue1.rename(columns={'ruas':'ruas_origin'})

        calculate_revenue1 = pd.merge(calculate_revenue1,port_ids, how='left', left_on=['destination'], right_on=['port']).drop(columns = ['port'])
        calculate_revenue1 = calculate_revenue1.rename(columns={'ruas':'ruas_destination'})
        matrix = copy.deepcopy(calculate_revenue1)
        
        matrix_notzero = matrix[(matrix['revenue']!=0)|(matrix['type']=='PASSENGER')]
        matrix_typelist = list(matrix_notzero['type'].unique())
        matrix_filtered = matrix[(matrix['type'].isin(matrix_typelist))]

        grouped_revenue = pd.DataFrame(calculate_revenue1.groupby(['type', 'origin','ruas_origin'])[['total', 'revenue']].sum()).reset_index()
        
        grouped_revenue = grouped_revenue.rename(columns={'ruas_origin':'ruas'})
                                
        # grouped_revenue = grouped_revenue[(grouped_revenue['revenue']!=0)|(grouped_revenue['type']=='PASSENGER')]
        
        notzero = grouped_revenue[(grouped_revenue['revenue']!=0)|(grouped_revenue['type']=='PASSENGER')]
        typelist = list(notzero['type'].unique())
        filtered = grouped_revenue[(grouped_revenue['type'].isin(typelist))]

        return filtered, matrix_filtered
    else:
        calculate_revenue1 = pd.DataFrame(columns=['type','origin','ruas','total','revenue'])
        matrix = pd.DataFrame(columns=['origin','destination','type','total','revenue','ruas_origin','ruas_destination'])
        return calculate_revenue1, matrix

# %%
def cal_revenue_with_dot(portlist, df_cargoflat, df_pricecargoconfig, df_lowpeak, df_basefare
                        , df_priceconfig, df_adjustment, cd_all, df_portdistance, dot_start):

    if len(unique(portlist))==len(portlist):
        revenue,matrix =  calculate_revenue_with_dot(unique(portlist), df_cargoflat, df_pricecargoconfig, df_lowpeak, df_basefare
                            , df_priceconfig, df_adjustment, cd_all, df_portdistance, dot_start)
        portDistance = cal_port_distance(portlist, df_portdistance)
        revenue = pd.merge(revenue,portDistance, how = 'inner', left_on=['origin'],right_on=['port']).drop(columns=['port'])
        return revenue, matrix
    else:
        portlist1,portlist2 = separate_portlist(portlist)
        cd_all1, cd_all2 = separate_demand(portlist1, portlist2, cd_all)
        if len(cd_all)==0:
            return pd.DataFrame()
        else:
            revenue1,matrix1 = calculate_revenue_with_dot(portlist1, df_cargoflat, df_pricecargoconfig, df_lowpeak, df_basefare
                                , df_priceconfig, df_adjustment, cd_all1, df_portdistance, dot_start)
            portdistance1 = cal_port_distance(portlist1, df_portdistance)
            revenue1 = pd.merge(revenue1,portdistance1, how = 'inner', left_on=['origin'],right_on=['port']).drop(columns=['port'])
            
            revenue2,matrix2 =  calculate_revenue_with_dot(portlist2, df_cargoflat, df_pricecargoconfig, df_lowpeak, df_basefare
                                , df_priceconfig, df_adjustment, cd_all2, df_portdistance, dot_start)
            portdistance2 = cal_port_distance(portlist2, df_portdistance)
            revenue2 = pd.merge(revenue2,portdistance2, how = 'inner', left_on=['origin'],right_on=['port']).drop(columns=['port'])
            
            start2 = math.floor(len(portlist)/2)
            revenue2.loc[:,'ruas']=revenue2.loc[:,'ruas']+start2
            revenue = pd.concat([revenue1,revenue2], ignore_index=True)
            
            matrix2.loc[:,'ruas_origin']=matrix2.loc[:,'ruas_origin']+start2
            matrix2.loc[:,'ruas_destination']=matrix2.loc[:,'ruas_destination']+start2
            matrix = pd.concat([matrix1,matrix2], ignore_index=True)
            return revenue,matrix  

# %%
def check_overlapping(start1, end1, start2, end2):
    if start2 <= start1 < end2 or start2 < end1 <= end2:
        return True
    elif start1 <= start2 < end1 or start1 < end2 <= end1:
        return True
    else:
        return False

# %%
def check_ispeak(start_date, end_date, df_lowpeak):
    peak_season = df_lowpeak[df_lowpeak['type'] == 'PEAK']

    for i in peak_season.index:
        peak_start_date = peak_season.loc[i, 'startdate']
        peak_end_date = peak_season.loc[i, 'enddate']
        check_overlap = check_overlapping(start_date, end_date, peak_start_date, peak_end_date)
        if check_overlap == True:
            return True
        elif check_overlap == False:
            continue
    
    return False

# %%
def check_isberth(idport, maxnoberth, start_date, end_date, all_schedule):
    schedule = all_schedule[all_schedule['idport'] == idport]
    berthed = 0
    if len(schedule)>0:
        for i in schedule.index:
            sch_start_date = schedule.loc[i, 'arrival_time']
            sch_end_date = schedule.loc[i, 'departure_time']
            check_overlap = check_overlapping(start_date, end_date, sch_start_date, sch_end_date)
            if check_overlap:
                berthed +=1
                if berthed>=maxnoberth:
                    return False
            
    if berthed<maxnoberth:
        return True

# %%
def schedule_cleaning(df_lowpeak, all_trip, all_schedule, all_revenue, all_cost, all_matrix):
    peak_season = df_lowpeak[df_lowpeak['type'] == 'PEAK']
    # pd.set_option('copy_on_write', True)
    # peak_season[['startdate', 'enddate']] = peak_season[['startdate', 'enddate']].apply(pd.to_datetime)

    trips = copy.deepcopy(all_trip)
    trips_peak = all_trip[all_trip['season'] == 'PEAK']
    trips_reg = all_trip[all_trip['season'] == 'REGULAR']

    trips_reg_rls = trips_reg[['idrls', 'voyage']]

    schedules = copy.deepcopy(all_schedule)
    revenues = copy.deepcopy(all_revenue)
    costs = copy.deepcopy(all_cost)
    matrixs = copy.deepcopy(all_matrix)
    
    # validation 1
    first_valid_rls_peak = pd.DataFrame(columns=['idrls', 'voyage'])

    for i in trips_peak.index:
        departure_time = trips_peak.loc[i, 'departuretime']
        arrival_time = trips_peak.loc[i, 'arrivaltime']
        if check_ispeak(departure_time, arrival_time, df_lowpeak):
            id_rls = trips_peak.loc[i, 'idrls']
            voyage = trips_peak.loc[i, 'voyage']
            add_first_valid_rls = pd.DataFrame({'idrls' : id_rls, 'voyage' : voyage}, index=[i])
            first_valid_rls_peak = pd.concat([first_valid_rls_peak, add_first_valid_rls])

    first_valid_rls = pd.concat([first_valid_rls_peak, trips_reg_rls]).reset_index(drop=True)

    first_valid_rls = first_valid_rls.drop_duplicates()
    
    trip_valid_1 = pd.merge(trips, first_valid_rls, how='inner', left_on=('idrls', 'voyage'), 
                                    right_on=('idrls', 'voyage'))    
    schedule_valid_1 = pd.merge(schedules, first_valid_rls, how='inner', left_on=('idrls', 'voyage'), 
                                    right_on=('idrls', 'voyage'))
    revenue_valid_1 = pd.merge(revenues, first_valid_rls, how='inner', left_on=('idrls', 'voyage'), 
                                    right_on=('idrls', 'voyage'))
    cost_valid_1 = pd.merge(costs, first_valid_rls, how='inner', left_on=('idrls', 'voyage'), 
                                    right_on=('idrls', 'voyage'))
    matrix_valid_1 = pd.merge(matrixs, first_valid_rls, how='inner', left_on=('idrls', 'voyage'), 
                                    right_on=('idrls', 'voyage'))
    # validation 2
    trip_valid_1_reg = trip_valid_1[trip_valid_1['season'] == 'REGULAR']
    trip_valid_1_peak = trip_valid_1[trip_valid_1['season'] == 'PEAK']

    clashed_reg_rls = pd.DataFrame(columns=['idrls', 'voyage'])
    for i in trip_valid_1_reg.index:
        for j in trip_valid_1_peak.index:
            reg_ship = trip_valid_1_reg.loc[i, 'idship']
            reg_departure_time = trip_valid_1_reg.loc[i, 'departuretime']
            reg_arrival_time = trip_valid_1_reg.loc[i, 'arrivaltime']
            reg_idrls = trip_valid_1_reg.loc[i, 'idrls']
            reg_voyage = trip_valid_1_reg.loc[i, 'voyage']

            peak_ship = trip_valid_1_peak.loc[j, 'idship']
            peak_departure_time = trip_valid_1_peak.loc[j, 'departuretime']
            peak_arrival_time = trip_valid_1_peak.loc[j, 'arrivaltime']

            if (reg_ship == peak_ship) & ((((reg_departure_time >= peak_departure_time) & (reg_departure_time <= peak_arrival_time))) | 
                                          (((reg_arrival_time >= peak_departure_time) & (reg_arrival_time <= peak_departure_time)))):
                # print(trip_valid_1_reg.loc[i, 'idrls'], trip_valid_1_reg.loc[i, 'voyage'], 'clashed with', 
                #       trip_valid_1_peak.loc[j, 'idrls'], trip_valid_1_peak.loc[j, 'voyage'])
                add_clashed_reg_rls = pd.DataFrame({'idrls' : reg_idrls, 'voyage' : reg_voyage}, index=[i])
                clashed_reg_rls = pd.concat([clashed_reg_rls, add_clashed_reg_rls])

    trips_valid_1_rls = trip_valid_1[['idrls', 'voyage']]
    
    trips_valid_1_rls = trips_valid_1_rls.drop_duplicates()
    clashed_reg_rls = clashed_reg_rls.drop_duplicates()

    second_valid_rls = pd.merge(trips_valid_1_rls, clashed_reg_rls, how='outer', on=('idrls', 'voyage'), indicator=True
                                ).query("_merge != 'both'").drop('_merge', axis=1).reset_index(drop=True)
    
    trip_valid_2 = pd.merge(trip_valid_1, second_valid_rls, how='inner', on=('idrls', 'voyage'))    
    schedule_valid_2 = pd.merge(schedule_valid_1, second_valid_rls, how='inner', on=('idrls', 'voyage'))    
    revenue_valid_2 = pd.merge(revenue_valid_1, second_valid_rls, how='inner', on=('idrls', 'voyage'))    
    cost_valid_2 = pd.merge(cost_valid_1, second_valid_rls, how='inner', on=('idrls', 'voyage'))    
    matrix_valid_2 = pd.merge(matrix_valid_1, second_valid_rls, how='inner', on=('idrls', 'voyage'))
    
    cleaned_trip = trip_valid_2
    cleaned_schedule = schedule_valid_2
    cleaned_revenue = revenue_valid_2
    cleaned_cost = cost_valid_2
    cleaned_matrix = matrix_valid_2

    return cleaned_trip, cleaned_schedule, cleaned_revenue, cleaned_cost, cleaned_matrix

# %%
def create_schedule(id_ship, idroute, listport, start_route, end_route, voyage, realcomm, season, all_schedule, dfr, df_maintenance1, 
                    df_rulecost, df_ship, df_port, df_portdistance, df_tide,df_cargoflat, df_pricecargoconfig, 
                    df_lowpeak, df_basefare, df_priceconfig, df_adjustment):
    route_trip = pd.DataFrame(columns=['idship', 'origin', 'destination', 'season', 'departuretime', 'arrivaltime', 'voyage', 'idrls'])
    route_schedule = pd.DataFrame(columns=['idship','idport','ruas','arrival_time','departure_time','voyage'])
    route_revenue = pd.DataFrame(columns=['port','ruas','up','down','onboard','factor','total','revenue','voyage'])
    route_cost = pd.DataFrame(columns=['idrulecost', 'pax', 'day', 'cost', 'voyage'])
    route_matrix = pd.DataFrame(columns=['origin','destination','type','total','voyage'])

    if len(listport)>1:
        origin = listport[0]
        destination = listport[-1]
            
        chosen_time = copy.deepcopy(decide_time(id_ship, listport, start_route, end_route, all_schedule, df_ship, df_port, df_portdistance, df_rulecost, dfr))
        if chosen_time==[]:
                chosen_time = copy.deepcopy(start_route)
                add_to = False
        else:
            if len(df_maintenance1)>0:
                start1 = copy.deepcopy(chosen_time)
                end1 = copy.deepcopy(start_route+pd.Timedelta(days=realcomm))
                for i,row in df_maintenance1.iterrows():
                    start2 = pd.Timestamp(row['start_docking_date'])
                    end2 = pd.Timestamp(row['end_docking_date'])
                    if check_overlapping(start1, end1, start2, end2):
                        add_to = False
                        break
                    else:
                        add_to = True
            else:
                add_to = True
            if add_to:
                voyage += 1
                print(f"Voyage ke-{voyage}")        
            
        arrtime = chosen_time
        if add_to:
            sched = calculate_list_schedule(id_ship, listport, chosen_time, all_schedule, df_ship,df_port, df_portdistance,'Y', df_tide)
            sched['voyage'] = voyage
            sched = sched.sort_values(by="ruas")
            # route_schedule = pd.concat([route_schedule, sched],ignore_index=True)

            deptime = sched.loc[sched['idport']==origin, 'arrival_time'].iloc[0]
            arrtime = sched.loc[sched['idport']==destination, 'departure_time'].iloc[-1]
            df_r = copy.deepcopy(dfr[(dfr['depdate'] >= deptime) & (dfr['depdate'] <= arrtime)])
            prediction = calculate_pax_prediction_with_dot(listport,df_r)
            cd_all = covered_demand_with_dot(prediction)

            factor,movement = cal_factor(id_ship, listport, cd_all, df_ship)

            if len(movement)>0:
                revenue,matrix = cal_revenue_with_dot(listport, df_cargoflat, df_pricecargoconfig, df_lowpeak, df_basefare,
                                                    df_priceconfig, df_adjustment, movement, df_portdistance, deptime)

                if (len(revenue)>0) & (len(factor)>0):# & (sum(revenue['revenue'])>0):
                    factor['voyage'] = voyage
                    revenue['voyage'] = voyage
                    matrix['voyage']= voyage
                    # df_join = pd.merge(revenue, factor, how='inner', left_on=('type','origin','ruas','voyage'), 
                    #                 right_on=('type','port','ruas','voyage')).drop(columns=['origin'])
                    merged1 = pd.merge(revenue,factor, how='right', left_on=['origin','type','ruas','voyage'], right_on=['port','type','ruas','voyage'])
                    maxruas = copy.deepcopy(max(merged1['ruas']))
                    merged1 = merged1.drop(list(merged1.loc[merged1['ruas'] == maxruas].index))
                    merged2 = pd.merge(factor,revenue, how='outer', left_on=['port','ruas','type','voyage'], right_on=['origin','ruas','type','voyage']).drop(columns = ['origin'])
                    merged1 = merged1.fillna(0)
                    merged2 = merged2.fillna(0)
                    if len(merged2)>0:
                        typelist = list(revenue['type'].unique())
                        if 'PASSENGER' not in typelist:
                            typelist = typelist.append('PASSENGER')
                        maxruas = copy.deepcopy(max(merged2['ruas']))
                        merged2 = merged2[(merged2['ruas']==maxruas)&(merged2['type'].isin(typelist))]
                        df_join = pd.concat([merged1,merged2], ignore_index=True)
                        filtered = df_join[(df_join['type'].isin(typelist))]
                    else:
                        filtered = merged1

                    route_revenue = pd.concat([route_revenue, filtered],ignore_index=True)
                    route_matrix = pd.concat([route_matrix, matrix],ignore_index=True)

                    cost = calculate_routecost_with_time(id_ship, listport, all_schedule, df_rulecost, df_ship, df_port, df_portdistance, factor, deptime)
                    cost['voyage'] = voyage
                    route_cost = pd.concat([route_cost, cost],ignore_index=True)
                
                    add_trip = pd.DataFrame({'idship' : id_ship, 'origin' : origin, 'destination' : destination, 'season' : season, 
                                        'departuretime' : deptime, 'arrivaltime' : arrtime, 'voyage': voyage, 'idrls': idroute}, index=[0])
                    route_trip = pd.concat([route_trip, add_trip], ignore_index=True)
                    route_schedule = pd.concat([route_schedule, sched],ignore_index=True)
                    
        new_start_route = chosen_time+pd.Timedelta(days=realcomm)
        if arrtime>new_start_route:
            new_start_route = arrtime
        new_end_route = copy.deepcopy(start_route+pd.Timedelta(days=realcomm))

        route_cost['idrls'] = idroute
        route_schedule['idrls'] = idroute
        route_revenue['idrls'] = idroute
        route_matrix['idrls'] = idroute

    return new_start_route,new_end_route,route_trip,route_schedule,route_revenue,route_cost,route_matrix,voyage

# %%
def fix_tripschedule(ship_trip, ship_schedule):
    maxvoyage = max(ship_trip['voyage'])
    ship_trip_fixed = copy.deepcopy(ship_trip.sort_values(by="voyage"))
    ship_schedule_fixed = copy.deepcopy(ship_schedule.sort_values(by=["voyage","ruas"]))
    for i,trip in ship_trip_fixed.iterrows():
        v = trip.voyage
        if v==maxvoyage:
            break
        else:
            av = v+1
            a_deptime = ship_trip.loc[(ship_trip.voyage==av),'departuretime'].iloc[0]
            #update arrivaltime on this voyage
            fill_time = a_deptime-pd.Timedelta(seconds=1)
            ship_trip_fixed.loc[(ship_trip_fixed.voyage==v),'arrivaltime'] = fill_time
            maxruas = max(ship_schedule.loc[(ship_schedule.voyage==v),'ruas'])
            ship_schedule_fixed.loc[((ship_schedule_fixed.voyage==v)&(ship_schedule_fixed.ruas==maxruas)),'departure_time'] = fill_time
            
    return ship_trip_fixed,ship_schedule_fixed            

# %%
def generate_schedule_beta_1(start, end, df_route, df_routedetail, df_ship, df_port, df_portdistance, 
                            df_revenue, df_rulecost, df_cargoflat, df_pricecargoconfig, df_lowpeak, df_basefare,
                            df_priceconfig, df_adjustment, df_maintenance, df_tide):
    date_start = pd.Timestamp(start)
    date_end = pd.Timestamp(end)
    
    df_lowpeak[['startdate', 'enddate']] = df_lowpeak[['startdate', 'enddate']].apply(pd.to_datetime)
    
    df_revenue['depdate'] = pd.to_datetime(df_revenue['depdate'])    
    df_revenue = df_revenue[(df_revenue['depdate'] >= date_start) & (df_revenue['depdate'] <= date_end)]

    all_trip = pd.DataFrame(columns=['idship', 'origin', 'destination', 'season', 'departuretime', 'arrivaltime', 'voyage', 'idrls'])
    all_schedule =  pd.DataFrame(columns=['idship','idport','ruas','arrival_time','departure_time','voyage','idrls'])
    all_revenue = pd.DataFrame(columns=['port','ruas','up','down','onboard','factor','total','revenue','voyage','idrls'])
    all_cost = pd.DataFrame(columns=['idrulecost', 'pax', 'day', 'cost', 'voyage', 'idrls'])
    all_matrix =  pd.DataFrame(columns=['origin','destination','type','total','voyage','idrls'])
    rt=0
    shiplist = unique(list(df_route.idship))
    size = len(shiplist)

    for s in shiplist:
        ship_trip = pd.DataFrame(columns=['idship', 'origin', 'destination', 'season', 'departuretime', 'arrivaltime', 'voyage', 'idrls'])
        ship_schedule =  pd.DataFrame(columns=['idship','idport','ruas','arrival_time','departure_time','voyage','idrls'])

        rt+=1
        dfrt = df_route[df_route['idship']==s]
        # dfrt = dfrt.sort_values(by="season", ascending=False)  #Regular season di generate di awal
        seasons = list(dfrt['season'])
        if 'REGULAR' in seasons:
            reg_route = dfrt[dfrt['season']=='REGULAR']
        else:
            reg_route = pd.DataFrame()

        if 'PEAK' in seasons:
            peak_route = dfrt[dfrt['season']=='PEAK']
        else:
            peak_route = pd.DataFrame()

        id_ship = s
            
        if len(reg_route)>0:
            idroute = reg_route['id'].iloc[0]
            # season = reg_route['season'].iloc[0]
            df_maintenance1 = df_maintenance[df_maintenance['id_ship']==id_ship]
            # commdays = route.commision
            route_det = copy.deepcopy(df_routedetail[(df_routedetail['type']=='PASSENGER') & (df_routedetail['idrls']==idroute)])
            route_det = route_det.sort_values(by=['ruas'])
            listport = list(route_det['idport'])
            
            realcomm = commission_days(id_ship, listport, df_ship, df_port, df_portdistance)
            start_route = copy.deepcopy(date_start)
            end_route = copy.deepcopy(start_route+pd.Timedelta(days=realcomm))
            voyage = 0
            while start_route<date_end:
                ispeak = check_ispeak(start_route,end_route, df_lowpeak)
                if (not ispeak) | ((ispeak) & (len(peak_route)==0)): #generate for regular schedule
                    
                    dfr = copy.deepcopy(df_revenue[(df_revenue['origin'].isin(listport)) & (df_revenue['destination'].isin(listport))])
                    if voyage==0:
                        aa = dfr[(pd.to_datetime(dfr['depdate']).between(start_route, end_route))]

                        while (len(aa)==0) & (start_route<date_end):
                            start_route = start_route+pd.Timedelta(days=realcomm)
                            end_route = copy.deepcopy(start_route+pd.Timedelta(days=realcomm))
                            aa = dfr[(pd.to_datetime(dfr['depdate']).between(start_route, end_route))]

                    sch = create_schedule(id_ship, idroute, listport, start_route, end_route, voyage, realcomm, 'REGULAR', all_schedule, dfr, df_maintenance1, 
                                                    df_rulecost, df_ship, df_port, df_portdistance, df_tide,df_cargoflat, df_pricecargoconfig, 
                                                    df_lowpeak, df_basefare, df_priceconfig, df_adjustment)
                else:
                    peak_idroute = peak_route['id'].iloc[0]
                    # commdays = route.commision
                    peak_route_det = copy.deepcopy(df_routedetail[(df_routedetail['type']=='PASSENGER') & (df_routedetail['idrls']==peak_idroute)])
                    peak_route_det = peak_route_det.sort_values(by=['ruas'])
                    peak_listport = list(peak_route_det['idport'])
                    peak_realcomm = commission_days(id_ship, peak_listport, df_ship, df_port, df_portdistance)
                    end_route = copy.deepcopy(start_route+pd.Timedelta(days=peak_realcomm))
                    
                    dfr = copy.deepcopy(df_revenue[(df_revenue['origin'].isin(peak_listport)) & (df_revenue['destination'].isin(peak_listport))])
                    if voyage==0:
                        aa = dfr[(pd.to_datetime(dfr['depdate']).between(start_route, end_route))]

                        while (len(aa)==0) & (start_route<date_end):
                            start_route = start_route+pd.Timedelta(days=realcomm)
                            end_route = copy.deepcopy(start_route+pd.Timedelta(days=realcomm))
                            aa = dfr[(pd.to_datetime(dfr['depdate']).between(start_route, end_route))]

                    sch = create_schedule(id_ship, peak_idroute, peak_listport, start_route, end_route, voyage, peak_realcomm, 'PEAK', all_schedule, dfr, df_maintenance1, 
                                                    df_rulecost, df_ship, df_port, df_portdistance, df_tide,df_cargoflat, df_pricecargoconfig, 
                                                    df_lowpeak, df_basefare, df_priceconfig, df_adjustment)
                start_route = sch[0]
                end_route = sch[1]
                voyage = sch[7]
                #OUTPUT; TO MAP TO JSON FORMAT
                ship_trip = pd.concat([ship_trip, sch[2]],ignore_index=True)
                ship_schedule = pd.concat([ship_schedule, sch[3]],ignore_index=True)
                all_revenue = pd.concat([all_revenue, sch[4]],ignore_index=True)
                all_cost = pd.concat([all_cost, sch[5]],ignore_index=True)
                all_matrix = pd.concat([all_matrix, sch[6]],ignore_index=True)
        else:
            peak_idroute = peak_route.id
            # commdays = route.commision
            peak_route_det = copy.deepcopy(df_routedetail[(df_routedetail['type']=='PASSENGER') & (df_routedetail['idrls']==peak_idroute)])
            peak_route_det = peak_route_det.sort_values(by=['ruas'])
            peak_listport = list(peak_route_det['idport'])
            
            peak_realcomm = commission_days(id_ship, peak_listport, df_ship, df_port, df_portdistance)
            start_route = copy.deepcopy(date_start)
            end_route = copy.deepcopy(start_route+pd.Timedelta(days=realcomm))
            voyage = 0
            dfr = copy.deepcopy(df_revenue[(df_revenue['origin'].isin(peak_listport)) & (df_revenue['destination'].isin(peak_listport))])
            if voyage==0:
                aa = dfr[(pd.to_datetime(dfr['depdate']).between(start_route, end_route))]

                while (len(aa)==0) & (start_route<date_end):
                    start_route = start_route+pd.Timedelta(days=peak_realcomm)
                    end_route = copy.deepcopy(start_route+pd.Timedelta(days=peak_realcomm))
                    aa = dfr[(pd.to_datetime(dfr['depdate']).between(start_route, end_route))]
                    
            while start_route<date_end:
                sch = create_schedule(id_ship, peak_idroute, peak_listport, start_route, end_route, voyage, peak_realcomm, 'PEAK', all_schedule, dfr, df_maintenance1, 
                                                df_rulecost, df_ship, df_port, df_portdistance, df_tide,df_cargoflat, df_pricecargoconfig, 
                                                df_lowpeak, df_basefare, df_priceconfig, df_adjustment)
            

                start_route = sch[0]
                end_route = sch[1]
                voyage = sch[7]
                #OUTPUT; TO MAP TO JSON FORMAT
                ship_trip = pd.concat([all_trip, sch[2]],ignore_index=True)
                ship_schedule = pd.concat([all_schedule, sch[3]],ignore_index=True)
                all_revenue = pd.concat([all_revenue, sch[4]],ignore_index=True)
                all_cost = pd.concat([all_cost, sch[5]],ignore_index=True)
                all_matrix = pd.concat([all_matrix, sch[6]],ignore_index=True)  
        if len(ship_trip)>0:
            ship_trip_fixed, ship_schedule_fixed = fix_tripschedule(ship_trip, ship_schedule) 
        else:
            ship_trip_fixed, ship_schedule_fixed = ship_trip, ship_schedule
        all_trip = pd.concat([all_trip, ship_trip_fixed],ignore_index=True)
        all_schedule = pd.concat([all_schedule, ship_schedule_fixed],ignore_index=True) 

        sp = round((rt/size)*100)
        print(f"Scheduling progress: {sp}%")

    return all_trip,all_schedule,all_revenue, all_cost,all_matrix

# %%
def generate_schedule_beta_1_0(start, end, df_route, df_routedetail, df_ship, df_port, df_portdistance, 
                            df_revenue, df_rulecost, df_cargoflat, df_pricecargoconfig, df_lowpeak, df_basefare,
                            df_priceconfig, df_adjustment, df_maintenance, df_tide):
    date_start = pd.Timestamp(start)
    date_end = pd.Timestamp(end)

    df_revenue['depdate'] = pd.to_datetime(df_revenue['depdate'])    
    df_revenue = df_revenue[(df_revenue['depdate'] >= date_start) & (df_revenue['depdate'] <= date_end)]

    all_trip = pd.DataFrame(columns=['idship', 'origin', 'destination', 'season', 'departuretime', 'arrivaltime', 'voyage', 'idrls'])
    all_schedule =  pd.DataFrame(columns=['idship','idport','ruas','arrival_time','departure_time','voyage','idrls'])
    all_revenue = pd.DataFrame(columns=['port','ruas','up','down','onboard','factor','total','revenue','voyage','idrls'])
    all_cost = pd.DataFrame(columns=['idrulecost', 'pax', 'day', 'cost', 'voyage', 'idrls'])
    all_matrix =  pd.DataFrame(columns=['origin','destination','type','total','voyage','idrls'])
    size = len(df_route)
    rt=0
    for r,route in df_route.iterrows():
        rt=rt+1
        route_trip = pd.DataFrame(columns=['idship', 'origin', 'destination', 'season', 'departuretime', 'arrivaltime', 'voyage', 'idrls'])
        route_schedule = pd.DataFrame(columns=['idship','idport','ruas','arrival_time','departure_time','voyage'])
        route_revenue = pd.DataFrame(columns=['port','ruas','up','down','onboard','factor','total','revenue','voyage'])
        route_cost = pd.DataFrame(columns=['idrulecost', 'pax', 'day', 'cost', 'voyage'])
        route_matrix = pd.DataFrame(columns=['origin','destination','type','total','voyage'])

        id_ship = route.idship
        idroute = route.id
        season = route.season

        df_maintenance1 = df_maintenance[df_maintenance['id_ship']==id_ship]
        # commdays = route.commision
        route_det = copy.deepcopy(df_routedetail[(df_routedetail['type']=='PASSENGER') & (df_routedetail['idrls']==idroute)])
        route_det = route_det.sort_values(by=['ruas'])
        listport = list(route_det['idport'])
        
        realcomm = commission_days(id_ship, listport, df_ship, df_port, df_portdistance)
        start_route = copy.deepcopy(date_start)
        end_route = copy.deepcopy(start_route+pd.Timedelta(days=realcomm))

        if len(listport)>1:
            origin = listport[0]
            destination = listport[-1]
            dfr = copy.deepcopy(df_revenue[(df_revenue['origin'].isin(listport)) & (df_revenue['destination'].isin(listport))])
            aa = dfr[(pd.to_datetime(dfr['depdate']).between(start_route, end_route))]

            while (len(aa)==0) & (start_route<date_end):
                start_route = start_route+pd.Timedelta(days=realcomm)
                end_route = copy.deepcopy(start_route+pd.Timedelta(days=realcomm))
                aa = dfr[(pd.to_datetime(dfr['depdate']).between(start_route, end_route))]
            
            if len(aa)>0:
                #route.commision
                chosen_time = copy.deepcopy(decide_time(id_ship, listport, start_route, end_route, df_ship, df_port, df_portdistance, df_rulecost, dfr))
                if chosen_time==[]:
                        chosen_time = copy.deepcopy(start_route)
                        add_to = False
                        voyage = 0
                else:
                    if len(df_maintenance1)>0:
                        start1 = copy.deepcopy(chosen_time)
                        end1 = copy.deepcopy(start_route+pd.Timedelta(days=realcomm))
                        for i,row in df_maintenance1.iterrows():
                            start2 = pd.Timestamp(row['start_docking_date'])
                            end2 = pd.Timestamp(row['end_docking_date'])
                            if check_overlapping(start1, end1, start2, end2):
                                add_to = False
                                break
                            else:
                                add_to = True
                    else:
                        add_to = True
                    if add_to:
                        voyage = 1
                        print(f"Voyage ke-{voyage}")        
                    else:
                        voyage = 0
                
                arrtime = chosen_time
                while chosen_time<date_end:
                    if add_to:
                        sched = calculate_list_schedule(id_ship, listport, chosen_time, df_ship,df_port, df_portdistance,'Y', df_tide)
                        sched['voyage'] = voyage
                        sched = sched.sort_values(by="ruas")
                        # route_schedule = pd.concat([route_schedule, sched],ignore_index=True)

                        deptime = sched.loc[sched['idport']==origin, 'arrival_time'].iloc[0]
                        arrtime = sched.loc[sched['idport']==destination, 'departure_time'].iloc[-1]
                        df_r = copy.deepcopy(dfr[(dfr['depdate'] >= deptime) & (dfr['depdate'] <= arrtime)])
                        prediction = calculate_pax_prediction_with_dot(listport,df_r)
                        cd_all = covered_demand_with_dot(prediction)
            
                        factor,movement = cal_factor(id_ship, listport, cd_all, df_ship)

                        if len(movement)>0:
                            revenue,matrix = cal_revenue_with_dot(listport, df_cargoflat, df_pricecargoconfig, df_lowpeak, df_basefare,
                                                                df_priceconfig, df_adjustment, movement, df_portdistance, deptime)

                            if (len(revenue)>0) & (len(factor)>0) & (sum(revenue['revenue'])>0):
                                factor['voyage'] = voyage
                                revenue['voyage'] = voyage
                                matrix['voyage']= voyage
                                # df_join = pd.merge(revenue, factor, how='inner', left_on=('type','origin','ruas','voyage'), 
                                #                 right_on=('type','port','ruas','voyage')).drop(columns=['origin'])
                                merged1 = pd.merge(revenue,factor, how='right', left_on=['origin','type','ruas','voyage'], right_on=['port','type','ruas','voyage'])
                                maxruas = copy.deepcopy(max(merged1['ruas']))
                                merged1 = merged1.drop(list(merged1.loc[merged1['ruas'] == maxruas].index))
                                merged2 = pd.merge(factor,revenue, how='outer', left_on=['port','ruas','type','voyage'], right_on=['origin','ruas','type','voyage']).drop(columns = ['origin'])
                                merged1 = merged1.fillna(0)
                                merged2 = merged2.fillna(0)
                                if len(merged2)>0:
                                    typelist = list(revenue['type'].unique())
                                    if 'PASSENGER' not in typelist:
                                        typelist = typelist.append('PASSENGER')
                                    maxruas = copy.deepcopy(max(merged2['ruas']))
                                    merged2 = merged2[(merged2['ruas']==maxruas)&(merged2['type'].isin(typelist))]
                                    df_join = pd.concat([merged1,merged2], ignore_index=True)
                                    filtered = df_join[(df_join['type'].isin(typelist))]
                                else:
                                    filtered = merged1

                                route_revenue = pd.concat([route_revenue, filtered],ignore_index=True)
                                route_matrix = pd.concat([route_matrix, matrix],ignore_index=True)

                                cost = calculate_routecost_with_time(id_ship, listport, df_rulecost, df_ship, df_port, df_portdistance, factor, deptime)
                                cost['voyage'] = voyage
                                route_cost = pd.concat([route_cost, cost],ignore_index=True)
                            
                                add_trip = pd.DataFrame({'idship' : id_ship, 'origin' : origin, 'destination' : destination, 'season' : season, 
                                                    'departuretime' : deptime, 'arrivaltime' : arrtime, 'voyage': voyage, 'idrls': idroute}, index=[0])
                                route_trip = pd.concat([route_trip, add_trip], ignore_index=True)
                                route_schedule = pd.concat([route_schedule, sched],ignore_index=True)
                    
                    start_route = chosen_time+pd.Timedelta(days=realcomm)
                    if arrtime>start_route:
                        start_route = arrtime

                    end_route = copy.deepcopy(start_route+pd.Timedelta(days=realcomm))

                    chosen_time = decide_time(id_ship, listport, start_route, end_route, df_ship, df_port, df_portdistance, df_rulecost,dfr)
                    if chosen_time==[]:
                        chosen_time = start_route
                        add_to = False
                    else:
                        if len(df_maintenance1)>0:
                            for i,row in df_maintenance1.iterrows():
                                start2 = pd.Timestamp(row['start_docking_date'])
                                end2 = pd.Timestamp(row['end_docking_date'])
                                if check_overlapping(start_route, end_route, start2, end2):
                                    add_to = False
                                    break
                                else:
                                    add_to = True
                        else:
                            add_to = True

                        if add_to: 
                            voyage += 1
                            print(f"Voyage ke-{voyage}")

            route_cost['idrls'] = idroute
            route_schedule['idrls'] = idroute
            route_revenue['idrls'] = idroute
            route_matrix['idrls'] = idroute
            
            #OUTPUT; TO MAP TO JSON FORMAT
            all_trip = pd.concat([all_trip, route_trip],ignore_index=True)
            all_schedule = pd.concat([all_schedule, route_schedule],ignore_index=True)
            all_revenue = pd.concat([all_revenue, route_revenue],ignore_index=True)
            all_cost = pd.concat([all_cost, route_cost],ignore_index=True)
            all_matrix = pd.concat([all_matrix, route_matrix],ignore_index=True)

            sp = round((rt/size)*100)
            print(f"Scheduling progress: {sp}%")

    all_revenue = all_revenue.drop(columns = ['origin'])
    df_lowpeak[['startdate', 'enddate']] = df_lowpeak[['startdate', 'enddate']].apply(pd.to_datetime)
    # return all_trip,all_schedule,all_revenue, all_cost,all_matrix
    clean_trip, clean_schedule,clean_revenue,clean_cost, clean_matrix=schedule_cleaning(df_lowpeak, all_trip, all_schedule, all_revenue, all_cost, all_matrix)
    return clean_trip, clean_schedule,clean_revenue,clean_cost, clean_matrix

# %%
def cleaned_port(df_port):
    port = copy.deepcopy(df_port)
    
    port['depth'] = port['depth'].astype(str).str.extract('(\d+)')
    port.loc[((port['depth'] == '') | (port['depth'] == None) | (port['depth'].isna())), 'depth'] = 10
    port['depth'] = port['depth'].astype(float)

    port['length'] = port['length'].astype(str).str.extract('(\d+)')
    port.loc[((port['length'] == '') | (port['length'] == None) | (port['length'].isna())), 'length'] = 200
    port['length'] = port['length'].astype(float)

    port['avgberth'] = port['avgberth'].astype(str).str.extract('(\d+)').astype(float)
    port.loc[((port['avgberth'] == '') | (port['avgberth'] == None) | (port['avgberth'].isna())), 'avgberth'] = 2
    
    port['lat'] = port['lat'].astype(str).str.extract('(\d+)')
    port.loc[((port['lat'] == '') | (port['lat'] == None) | (port['lat'].isna())), 'lat'] = 24
    port['lat'] = port['lat'].astype(float)

    port['long'] = port['long'].astype(str).str.extract('(\d+)')
    port.loc[((port['long'] == '') | (port['long'] == None) | (port['long'].isna())), 'long'] = (-96)
    port['long'] = port['long'].astype(float)
    
    port['noberth'] = port['noberth'].astype(str).str.extract('(\d+)')
    port.loc[((port['noberth'] == '') | (port['noberth'] == None) | (port['noberth'].isna())), 'noberth'] = 1
    port['noberth'] = port['noberth'].astype(int)

    for i,p in port.iterrows():
        port.loc[i,'minberthtime'] = str(port.loc[i,'minberthtime'])
        port.loc[i,'maxberthtime'] = str(port.loc[i,'maxberthtime'])
    minberthtime = '00:00:00'
    minberthtime = datetime.strptime(minberthtime, '%H:%M:%S')
    port.loc[((port['minberthtime'] == 0) | (port['minberthtime'] == 'None') | (port['minberthtime'].isna())), 'minberthtime'] = minberthtime
    port['minberthtime'] = pd.to_datetime(port['minberthtime'])
    port['minberthtime'] = port['minberthtime'].dt.time
        
    maxberthtime = '23:59:59'
    maxberthtime = datetime.strptime(maxberthtime, '%H:%M:%S')
    port.loc[((port['maxberthtime'] == 0) | (port['maxberthtime'] == 'None') | (port['maxberthtime'].isna())), 'maxberthtime'] = maxberthtime
    port['maxberthtime'] = pd.to_datetime(port['maxberthtime'])
    port['maxberthtime'] = port['maxberthtime'].dt.time

    return port


