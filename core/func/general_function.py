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
import itertools

# %%
# %store -r df_port
# %store -r df_ship
# %store -r df_portdistance
# %store -r df_revenue
# %store -r df_pricecargoconfig
# %store -r df_lowpeak
# %store -r df_basefare
# %store -r df_rulecost
# %store -r df_priceconfig
# %store -r df_cargoflat
# %store -r df_adjustment

# %%
def input_df(json_string):
    route_detail = json_string['parameter']['data']['routeDetail']
    forbidden_port = json_string['parameter']['data']['forbiddenPort']
    idConf = json_string['parameter']['data']['idConfRoute']
    try:
        df_input = pd.DataFrame(route_detail)
    except pd.errors.IndexingError:
        print("Data can't be read or is not valid JSON.")
    
    return idConf,forbidden_port, df_input

# %%


# %%
def unique(lists):
    unique_list = pd.Series(lists).drop_duplicates().tolist()
    return list(unique_list)

# %%
def separate_rev(df_revenue, df_lowpeak):
    df_revenue['depdate'] = pd.to_datetime(df_revenue['depdate'])    
    df_lowpeak['startdate'] = pd.to_datetime(df_lowpeak['startdate'])
    df_lowpeak['enddate'] = pd.to_datetime(df_lowpeak['enddate'])
    peak_season = df_lowpeak[df_lowpeak['type'] == 'PEAK'].reset_index(drop=True)
    # low_season = df_lowpeak[df_lowpeak['type'] == 'LOW'].reset_index(drop=True)

    df_peakrev = pd.DataFrame()
    for idx in peak_season.index:
        peak_revenue = df_revenue[(df_revenue['depdate'] >= peak_season['startdate'][idx]) & (df_revenue['depdate'] <= peak_season['enddate'][idx])]
        if not peak_revenue.empty:
            df_peakrev = pd.concat([df_peakrev, peak_revenue])

    df_peakrev = df_peakrev.reset_index(drop=True)

    if not df_peakrev.empty:
        df_lowrev = pd.concat([df_revenue, df_peakrev]).drop_duplicates(keep=False).reset_index(drop=True)
    else:
        df_lowrev = df_revenue.copy()

    return df_lowrev,df_peakrev

# %%
def notoid(gen, df_port):
    #global df_port
    a=df_port.loc[df_port['code'].isin(gen),['code','id']]
    idgen=[]
    for i in gen:
        idgen.append(a.id[a.code==i].values[0])

    return idgen

# %%
def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return unique(lst3)

def not_intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value not in lst2]
    return lst3

def fix_mustvisit(mustvisit,gen, ori, des):
    ins = not_intersection(mustvisit,gen)
    if len(ins)>0:
        if (ori!='') & (des!=''):
            shuf = copy.deepcopy(gen[1:-1])
        elif (ori!='') & (des==''):
            shuf = copy.deepcopy(gen[1:])
        elif (ori=='') & (des!=''):
            shuf = copy.deepcopy(gen[0:-1])
        else:
            shuf = copy.deepcopy(gen)
        for i in ins:
            r=not_intersection(shuf,mustvisit)
            if len(r)>0:
                shuffle(r)
                change = gen.index(r[0])
                gen[change]=i
                shuf.remove(r[0])
                #if hard_fitness(row.id,gen,maxdays)
    return gen

# %%
def port_mustvisit(mv, df_port):
    mustvisit = []
    if mv!='':
        mv = mv.split(',')
        for i in mv:
            new_mv = i.replace(" ","")
            if new_mv!='':
                nmv = df_port.loc[df_port.id==new_mv,'id'].values[0]
                mustvisit.append(nmv)
    return mustvisit

# %%
def region_rule(listport, df_port):
    #global df_port
    
    df = df_port.loc[df_port['id'].isin(listport),['id', 'region']]
    uniq_number = df['region'].nunique()

    if uniq_number == 1:
        region_rule = 0
    elif uniq_number == 2:
        region_rule = 1
    elif uniq_number > 2:
        region_rule = 2
    
    data = {'region_rule' : region_rule}
    return data

# %%
def calculate_totaldistance(id_port, df_portdistance):
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
    total_commercial = df_join['commercial'].sum()
    data = {'total_nautical' : total_nautical,
            'total_commercial' : total_commercial}
    df = pd.DataFrame(data, index=[0])
    return df

# %%
def calculate_totdistance(id_port, df_portdistance):
    
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
def calculate_time(id_ship, id_port, df_ship, df_port, df_portdistance):
    #global df_ship, df_port, df_portdistance
    
    df_distance = calculate_totaldistance(id_port, df_portdistance)
    df_ship1 = df_ship[df_ship['id'] == id_ship]
    
    df_distance1 = df_distance.fillna(0)
    sailing_time = (df_distance1.iloc[0]['total_nautical']/df_ship1.iloc[0]['speed'])/24
    
    df_port1 = df_port[df_port['id'].isin(id_port)]
    berthing_time = df_port1['avgberth'].sum()/24

    comm_days = round(berthing_time + sailing_time)
    
    data = {'Name' : ['Sailing Time', 'Berthing Time', 'Commission Days'],
            'Amount' : [sailing_time, berthing_time, comm_days]}
    df = pd.DataFrame(data)
    return df

# %%
def commission_days(id_ship, listport, df_ship, df_port, df_portdistance):
    
    df_ship1 = df_ship[df_ship['id'] == id_ship]

    df_port1 = df_port[df_port['id'].isin(listport)]
    berthing_time = df_port1['avgberth'].sum()

    total_distance = pd.DataFrame(calculate_totaldistance(listport, df_portdistance))
    sailing_time = total_distance.iloc[0]['total_nautical']/df_ship1.iloc[0]['speed']
    
    comm_days = round((sailing_time + berthing_time)/24, 0)

    return comm_days

# %%
def hard_fitness(idship,gen,maxdays,mustvisit, df_port, df_ship, df_portdistance):
    idgen=copy.deepcopy(gen)
    comdays= commission_days(idship,idgen,df_ship,df_port,df_portdistance)
    if comdays>maxdays:
        return False
    else:
        if len(intersection(gen,mustvisit))!=len(mustvisit):
            return False
        else:
            return True

# %%
def normalize(l):
    maxl=max(l)
    minl=min(l)
    r = maxl-minl
    if r==0:
        nor = [0 for i in l]
    else:
        nor = [(i-minl)/r for i in l]
        l0 = [i==0 for i in l]
        for j,val in enumerate(l0):
            if val:
                nor[j]=0
    return nor

# %%
def normalized_mean(ship_input, norm_dist, norm_tot, norm_cov,norm_profit):    
    norm_mean = []
    for j in range(0,len(ship_input)):
        n = []
        for i in range(0,len(norm_dist[j])):
            weighted_mean = ((norm_dist[j][i]+norm_tot[j][i]+norm_cov[j][i])*0.15)+(norm_profit[j][i]*0.55)
            n.append(weighted_mean)
        norm_mean.append(n)
    return norm_mean

# %%
def calculate_prediction(id_port, df_rev):
    #global df_revenue, df_lowpeak
    
    # df_revenue['depdate'] = pd.to_datetime(df_revenue['depdate'])    
    # df_lowpeak['startdate'] = pd.to_datetime(df_lowpeak['startdate'])
    # df_lowpeak['enddate'] = pd.to_datetime(df_lowpeak['enddate'])
    # peak_season = df_lowpeak[df_lowpeak['type'] == 'PEAK'].reset_index(drop=True)
    # #low_season = df_lowpeak[df_lowpeak['type'] == 'LOW'].reset_index(drop=True)

    # df_peakrev = pd.DataFrame()
    # for idx in peak_season.index:
    #     peak_revenue = df_revenue[(df_revenue['depdate'] >= peak_season['startdate'][idx]) & (df_revenue['depdate'] <= peak_season['enddate'][idx])]
    #     if not peak_revenue.empty:
    #         df_peakrev = pd.concat([df_peakrev, peak_revenue])

    # df_peakrev = df_peakrev.reset_index(drop=True)

    # if not df_peakrev.empty:
    #     df_lowrev = pd.concat([df_revenue, df_peakrev]).drop_duplicates(keep=False).reset_index(drop=True)

    # if season == 'PEAK':
    #     df_rev = df_peakrev
    # elif season == 'LOW':
    #     df_rev = df_lowrev

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
    
    if len(df_rev)==0:
        df_join = pd.DataFrame({'origin' : origin, 'destination' : destination, 'depdate': '1990-01-01', 'deptime':'1990-01-01',
                            'revenue':0, 'total':0, 'type':'PASSENGER'})
    else:        
        od_pair = pd.DataFrame({'origin' : origin, 'destination' : destination})
        df_joinpax = pd.merge(df_rev[df_rev.type=='PASSENGER'], od_pair, how='right', left_on=('origin', 'destination'), right_on=('origin', 'destination'))
        df_joinpax.loc[df_joinpax['type'].isnull(),'type'] ='PASSENGER'
        df_joinpax.loc[df_joinpax['total'].isnull(),'total'] =0
        df_joincargo = pd.merge(df_rev[df_rev.type!='PASSENGER'], od_pair, how='inner', left_on=('origin', 'destination'), right_on=('origin', 'destination'))
        df_join = pd.concat([df_joinpax,df_joincargo], ignore_index=True)
                
        # if 'PASSENGER' not in list(df_join['type'].unique()):
        #     df_pax = copy.deepcopy(df_join)
        #     df_pax.loc[:,'type']='PASSENGER'
        #     df_pax.loc[:,'total']=0
        #     df_join = pd.concat([df_join,df_pax], ignore_index=True)

    df = df_join.groupby(['origin', 'destination', 'type'], as_index=False)[['revenue', 'total']].describe()
    return df

# %%
def calculate_routecost(id_ship, id_port, npax, df_rulecost, df_ship, df_port, df_portdistance):
    #global df_rulecost
    
    rulecosts = copy.deepcopy(df_rulecost[df_rulecost['localtime'].isnull()])
    rulecosts['ispax'] = rulecosts['ispax'].fillna(False)
    rulecosts['issailing'] = rulecosts['issailing'].fillna(False)
    rulecosts['isberthing'] = rulecosts['isberthing'].fillna(False)
    rulecosts['perday'] = rulecosts['perday'].fillna(np.nan).replace([np.nan], [None])

    totalcost = 0

    costdetail = pd.DataFrame(columns=['idrulecost', 'name', 'npax','day', 'sailingtime', 'berthingtime', 'idship', 'idport', 'time', 'expenseperday', 'cost'])

    for id in rulecosts.index:
        ispax = rulecosts['ispax'][id]
        issail = rulecosts['issailing'][id]
        isberth = rulecosts['isberthing'][id]
        name = rulecosts['name'][id]
        costrule = rulecosts['cost'][id]
        idrulecost = rulecosts['idrulecost'][id]
        ship_rule = rulecosts['idship'][id]
        port_rule = rulecosts['idport'][id]
        perday = rulecosts['perday'][id]

        if (ispax == False) & (issail == False) & (isberth == False):
            comm_days = commission_days(id_ship, id_port, df_ship, df_port, df_portdistance)

            if perday == None:
                expense_perday = 1
                fillday = 0
            else:
                expense_perday = perday
                fillday = comm_days

            if (ship_rule == None) & (port_rule == None):
                cost = costrule * comm_days * expense_perday
                add_cost = pd.DataFrame({'idrulecost' : idrulecost, 'name' : name, 'cost' : cost, 'day':fillday}, index=[id])
                if not add_cost.empty:
                    costdetail = pd.concat([costdetail, add_cost])
            else: 
                if (ship_rule == None):
                    if (port_rule != None):
                        for i in id_port:
                            if (port_rule == i):
                                cost = costrule * comm_days * expense_perday
                                add_cost = pd.DataFrame({'idrulecost' : idrulecost, 'name' : name, 'cost' : cost, 
                                                         'idship' : ship_rule, 'idport' : port_rule, 'day':fillday}, index=[id])
                                if not add_cost.empty:
                                    costdetail = pd.concat([costdetail, add_cost])
                else:
                    if (ship_rule == id_ship):                 
                        cost = costrule * comm_days * expense_perday
                        add_cost = pd.DataFrame({'idrulecost' : idrulecost, 'name' : name, 'cost' : cost, 'idship' : id_ship, 'day':fillday}, 
                                                index=[id])
                        if not add_cost.empty:
                            costdetail = pd.concat([costdetail, add_cost])
                
        else:
            timecal = pd.DataFrame(calculate_time(id_ship, id_port, df_ship, df_port, df_portdistance))
            timecal.set_index('Name', inplace=True)
            sailing_time = timecal.loc['Sailing Time']['Amount']
            berthing_time = timecal.loc['Berthing Time']['Amount']

            if isberth == True:
                if berthing_time != 0:
                    cost = berthing_time * perday* costrule
                    
                    expenseperday = perday
                    add_cost = pd.DataFrame({'idrulecost':idrulecost, 'name' : name, 'idship' : ship_rule, 
                                             'berthingtime' : berthing_time, 'expenseperday' : expenseperday, 
                                             'cost' : cost, 'day':berthing_time}, index=[id])
                    if not add_cost.empty:
                        costdetail = pd.concat([costdetail, add_cost])
                else:
                    continue

            elif issail == True:
                if sailing_time != 0:
                    cost = sailing_time * perday * costrule

                    expenseperday = perday
                    add_cost = pd.DataFrame({'idrulecost':idrulecost, 'name' : name, 'idship' : ship_rule, 
                                             'sailingtime' : sailing_time, 'expenseperday' : expenseperday, 
                                             'cost' : cost, 'day':sailing_time}, index=[id])
                    if not add_cost.empty:
                        costdetail = pd.concat([costdetail, add_cost])
                else:
                    continue

            elif ispax == True:
                if len(id_port) <= 1:
                    continue
                else:
                    comm_days = commission_days(id_ship, id_port, df_ship, df_port, df_portdistance)
                    if perday == None:
                        expense_perday = 1
                        fillday = 0
                    else:
                        expense_perday = perday
                        fillday = comm_days

                    cost = npax * expense_perday * comm_days* df_rulecost['cost']

                    add_cost = pd.DataFrame({'idrulecost' : idrulecost, 'name' : name, 'idship' : ship_rule, 
                                             'npax' : npax, 'cost' : cost, 'day':fillday}, index=[id])
                    if not add_cost.empty:
                        costdetail = pd.concat([costdetail, add_cost])

    totalcost = round(costdetail['cost'].sum(), 2)
    data = {'totalcost' : totalcost}
    total = pd.DataFrame(data, index=[0])

    return costdetail, total

# %%
def calculate_cargoprice(distance, dot, df_pricecargoconfig, df_lowpeak, df_basefare, cargotype, season=None):
    #global df_pricecargoconfig, df_lowpeak, df_basefare
    
    df_pricecargoconfig1 = df_pricecargoconfig[(df_pricecargoconfig['mindistance'] <= distance) & (df_pricecargoconfig['maxdistance'] >= distance)]
    df_basefare1 = df_basefare[df_basefare['typefare'] == cargotype]

    if dot is None:
        if season is None:
            fare = 0
        else:
            df_basefare1 = df_basefare1[df_basefare1['type'] == season]
            fare = df_basefare1['basefare'].mean()
    else:
        dot_date = pd.Timestamp(dot)
        df_lowpeak1 = df_lowpeak[(df_lowpeak['startdate'] <= dot_date) & (df_lowpeak['enddate'] >= dot_date)]
        #dot_datetime = datetime.datetime.strptime(dot, '%Y-%m-%d')
        df_basefare1 = df_basefare1[(df_basefare1['startdate'] <= dot_date) & (df_basefare1['enddate'] >= dot_date)]
        df_basefare1 = df_basefare1[df_basefare1['type'] == df_lowpeak1.iloc[0]['type']]
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
def calculate_paxprice(distance, dot, df_priceconfig, df_lowpeak, df_basefare, df_adjustment, season=None):
    #global df_priceconfig, df_lowpeak, df_basefare, df_adjustment
    
    df_priceconfig1 = df_priceconfig[(df_priceconfig['mindistance'] <= distance) & (df_priceconfig['maxdistance'] >= distance)]
    df_basefare1 = df_basefare[df_basefare['typefare'] == 'PASSENGER']
    if dot is None:
        if season is None:
            fare = 0
        else:        
            df_basefare1 = df_basefare1[df_basefare1['type'] == season]
            fare = df_basefare1['basefare'].mean()
    else:
        dot_date = pd.Timestamp(dot)
        df_lowpeak1 = df_lowpeak[(df_lowpeak['startdate'] <= dot_date) & (df_lowpeak['enddate'] >= dot_date)]
        #dot_datetime = datetime.datetime.strptime(dot, '%Y-%m-%d')
        df_basefare1 = df_basefare1[(df_basefare1['startdate'] <= dot_date) & (df_basefare1['enddate'] >= dot_date)]
        df_basefare1 = df_basefare1[df_basefare1['type'] == df_lowpeak1.iloc[0]['type']]
        fare = df_basefare1.iloc[0]['basefare']
    
    if len(df_priceconfig1)>0:
        if df_priceconfig1.iloc[0]['mindistance'] == 0:
            koefjarak = df_priceconfig1['distancecoeff']
        elif df_priceconfig1.iloc[0]['mindistance'] !=0:
            koefjarak = df_priceconfig1.iloc[0]['distancecoeff'] + ((distance - df_priceconfig1.iloc[0]['mindistance'] - 1) * df_priceconfig1.iloc[0]['coeff'])
    else:
        koefjarak = 0
        
    rp = round(koefjarak * fare, -3)

    if distance < 1000:
        pnp = rp
    elif distance >= 1000:
        pnp = ((100+df_adjustment)/100) * rp

    pnpmile = round(pnp/distance, 2)
    if len(df_priceconfig1)>0:
        tarif = round(distance * (df_priceconfig1.iloc[0]['coeff']) * (df_priceconfig1.iloc[0]['pangsa']) * pnpmile, -3)
    else:
        tarif = 0
    tarifakhir = round(0.97 * tarif, -3)

    data = {'Name' : ['Tarif', 'Tarif Akhir'],
            'Amount' : [tarif, tarifakhir]}
    df = pd.DataFrame(data)
    return df

# %%
def covered_demand(covered_demand1, season='LOW'):
    # if covered_demand1 is None:
    #     covered_demand1 = calculate_prediction(portlist, df_revenue, df_lowpeak, season)
    
    if len(covered_demand1)>0:    
        movements_list = []
        for index, row in covered_demand1.iterrows():
            source_port = str(row.iloc[0]).replace("Name: 0, dtype: object", "").strip()
            dest_port = str(row.iloc[1]).replace("Name: 0, dtype: object", "").strip()
            cargo_type = str(row.iloc[2]).replace("Name: 0, dtype: object", "").strip()
            if season=='PEAK':
                median = round(row['total']['max'])
            else:
                median = round(row['total']['mean'])

            movement_data = {
                'origin': source_port,
                'destination': dest_port,
                'type': cargo_type,
                'total': median
            }
            movements_list.append(movement_data)
        movement_df = pd.DataFrame(movements_list)
        movement_df = movement_df.groupby(['origin','destination','type'], as_index=False)['total'].sum()
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
def calculate_distanceperport(id_port, df_portdistance): 
    #global df_portdistance
    
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
    
    df_cargoflat = df_portdistance
    df_join = pd.merge(df_cargoflat, od_pair, how='inner', left_on=('id_origin', 'id_destination'), right_on=('id_origin', 'id_destination')).drop(columns = ['commercial'])
    return df_join

# %%
def calculate_revenue(portList, dot, movement_df, df_cargoflat, df_pricecargoconfig, df_lowpeak, df_basefare,
                     df_priceconfig, df_adjustment, df_portdistance, season='LOW'):
    #global df_cargoflat
    cal_distance = pd.DataFrame(calculate_distanceperport(portList, df_portdistance))
    cal_distance = cal_distance.drop(columns=['origin', 'destination'])
    cal_distance = cal_distance.rename(columns={'id_origin' : 'origin', 'id_destination' : 'destination'})

    if len(movement_df)>0:
        calculate_revenue1 = pd.DataFrame(columns=['origin', 'destination', 'type','total','revenue'])
        distance_prediction = pd.merge(movement_df, cal_distance, on=('origin','destination'), how='inner')
        distance_prediction = distance_prediction[distance_prediction['type'].notna()]

        cargoflat = df_cargoflat
        cargoflat.loc[cargoflat['type'] == 'DRY', 'type'] = 'DRY_CONTAINER'
        cargoflat.loc[cargoflat['type'] == 'REEFER', 'type'] = 'REEFER_CONTAINER'
        cargoflat_m = cargoflat.rename(columns={'id_origin' : 'origin', 'id_destination' : 'destination', 'fare' : 'cargoflat_fare'})
        # cargoflat_m.columns = pd.MultiIndex.from_product([list(cargoflat_m.columns), ['']])

        distance_prediction_cargoflat = pd.merge(distance_prediction, cargoflat_m, how='left', left_on=('origin','destination','type'), right_on=('origin','destination','type'))
        
        for i in distance_prediction_cargoflat.index:
            distance = distance_prediction_cargoflat.loc[i, 'nautical']
            origin = distance_prediction_cargoflat.loc[i, 'origin']
            destination = distance_prediction_cargoflat.loc[i, 'destination']
            type = distance_prediction_cargoflat.loc[i, 'type']
            total_median = round(distance_prediction_cargoflat.loc[i,'total'])
                
            if type == 'PASSENGER':
                cal_paxprice = pd.DataFrame(calculate_paxprice(distance, None, df_priceconfig, df_lowpeak, df_basefare, df_adjustment, season))
                cal_paxprice.set_index('Name', inplace=True)
                price = cal_paxprice['Amount']['Tarif Akhir']
            elif type == 'GENERAL_CARGO':
                cal_cargoprice = calculate_cargoprice(distance, None, df_pricecargoconfig, df_lowpeak, df_basefare, 'CARGO', season)
                #cal_cargoprice.set_index('Name', inplace=True)
                price = cal_cargoprice
            elif type == 'REDPACK':
                cal_cargoprice = calculate_cargoprice(distance, None, df_pricecargoconfig, df_lowpeak, df_basefare, 'REDPACK', season)
                #cal_cargoprice.set_index('Name', inplace=True)
                price = cal_cargoprice
            else:
                price = distance_prediction_cargoflat.loc[i, 'cargoflat_fare']
            
            revenue = price * total_median
            add_revenue = pd.DataFrame({'origin' : origin, 'destination' : destination, 'type' : type, 'total' : total_median, 'revenue' : revenue}, index=[i])
            if not add_revenue.empty:
                calculate_revenue1 = pd.concat([calculate_revenue1, add_revenue])

        calculate_revenue1['revenue'] = calculate_revenue1['revenue'].fillna(0)

        port_ids = [list([port,i]) for i, port in enumerate(portList)]
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
        
        notzero = grouped_revenue[(grouped_revenue['revenue']!=0)|(grouped_revenue['type']=='PASSENGER')]
        typelist = list(notzero['type'].unique())
        filtered = grouped_revenue[(grouped_revenue['type'].isin(typelist))]
        # grouped_revenue = grouped_revenue.drop(list(grouped_revenue.loc[grouped_revenue['revenue']==0].index))
        
        return filtered, matrix_filtered
    else:
        calculate_revenue1 = pd.DataFrame(columns=['type','origin','ruas','total','revenue'])
        matrix = pd.DataFrame(columns=['origin','destination','type','total','revenue','ruas_origin','ruas_destination'])
        return calculate_revenue1,matrix

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
def cal_revenue(portList, dot, cd_all, df_cargoflat, df_pricecargoconfig, df_lowpeak, df_basefare
                , df_priceconfig, df_adjustment, df_portdistance, season='LOW'):

    if len(unique(portList))!=len(portList):  
        portlist1,portlist2 = separate_portlist(portList)
        
        cd_all1, cd_all2 = separate_demand(portlist1, portlist2, cd_all)
        revenue1,matrix1 = calculate_revenue(portlist1, None, cd_all1, df_cargoflat, df_pricecargoconfig, df_lowpeak, df_basefare,
                                      df_priceconfig, df_adjustment, df_portdistance, season)
        portdistance1 = cal_port_distance(portlist1, df_portdistance)
        revenue1 = pd.merge(revenue1,portdistance1, how = 'inner', left_on=['origin'],right_on=['port']).drop(columns=['port'])
        start2 = copy.deepcopy(max(revenue1['ruas']))+1
        revenue2,matrix2 =  calculate_revenue(portlist2, None, cd_all2, df_cargoflat, df_pricecargoconfig, df_lowpeak, df_basefare,
                                      df_priceconfig, df_adjustment, df_portdistance, season)
        portdistance2 = cal_port_distance(portlist2, df_portdistance)
        revenue2 = pd.merge(revenue2,portdistance2, how = 'inner', left_on=['origin'],right_on=['port']).drop(columns=['port'])
        revenue2.loc[:,'ruas']=revenue2.loc[:,'ruas']+start2
        revenue = pd.concat([revenue1,revenue2], ignore_index=True)
        
        matrix2.loc[:,'ruas_origin']=matrix2.loc[:,'ruas_origin']+start2
        matrix2.loc[:,'ruas_destination']=matrix2.loc[:,'ruas_destination']+start2
        matrix = pd.concat([matrix1,matrix2], ignore_index=True)

    else:
        revenue,matrix =  calculate_revenue(portList, dot, cd_all, df_cargoflat, df_pricecargoconfig, df_lowpeak, df_basefare,
                                      df_priceconfig, df_adjustment, df_portdistance, season)
        portDistance = cal_port_distance(portList, df_portdistance)
        revenue = pd.merge(revenue,portDistance, how = 'inner', left_on=['origin'],right_on=['port']).drop(columns=['port'])

    return revenue,matrix

# %%
def ship_capacity(idship, df_ship):
    #global df_ship

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
def cal_coverage(idShip, portList, movement_df, df_ship):
    
    capacity = ship_capacity(idShip, df_ship)
    output = pd.DataFrame(columns=['port','ruas','type','in','out','onboard','coverage'])
        
    if len(movement_df)>0:
        # Create initial IDs for source and destination ports
        port_ids = [list([port,i]) for i, port in enumerate(portList)]
        port_ids = pd.DataFrame(list(port_ids), columns=['port','ruas'])

        cargo_types = ['DRY_CONTAINER', 'REEFER_CONTAINER', 'TRUK', 'MOBIL', 'MOTOR', 'REDPACK', 'GENERAL_CARGO', 'PASSENGER']
        for no, port_name in port_ids.iterrows():
            insum = movement_df[movement_df['source_id'] == port_name['ruas']][['type','total']]
            in_sum = insum.groupby(['type'], as_index=False)['total'].sum()
            in_sum = in_sum.rename(columns={'total':'in'})

            outsum = movement_df[movement_df['destination_id'] == port_name['ruas']][['type','total']]
            out_sum = outsum.groupby(['type'], as_index=False)['total'].sum()
            out_sum = out_sum.rename(columns={'total':'out'})

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
                                down=b.loc[b['type'] == cargo_type, 'out'].iloc[0]
                                if len(before)>0:
                                    b_onb=before['onboard'].iloc[0]
                                    if down>b_onb:
                                        down = b_onb
                                else:
                                    b_onb=0
                                up=b.loc[b['type'] == cargo_type, 'in'].iloc[0]
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
                            b.loc[b['type'] == cargo_type, 'in'] = up
                            b.loc[b['type'] == cargo_type, 'out'] = down
                            b.loc[b['type'] == cargo_type, 'onboard'] = onb   
                        b.loc[b['type'] == cargo_type, 'coverage'] = lf
                if len(b)>0:
                    b.loc[:,'port'] = port_name['port']
                    b.loc[:,'ruas'] = port_name['ruas']
                
            if not b.empty:
                output = pd.concat([output,b], ignore_index=True)

    return output.fillna(0),movement_df

# %%


# %%
def cal_factor(idShip, portList, cd_all, df_ship):

    if len(unique(portList))!=len(portList): #check roundtrip
        portlist1,portlist2 = separate_portlist(portList)
        movement_df1, movement_df2 = separate_demand(portlist1, portlist2, cd_all)
        factor1,movement1 = cal_coverage(idShip, portlist1, movement_df1, df_ship)
        factor2,movement2 = cal_coverage(idShip, portlist2, movement_df2, df_ship)
        if len(movement1)==0:
            return pd.DataFrame(), pd.DataFrame()
        maxruas = copy.deepcopy(max(factor1['ruas']))
        minruas = 0
        keep1 = copy.deepcopy(factor1[factor1['ruas']==maxruas])
        factor1 = factor1[factor1['ruas']!=maxruas]
        keep2 = copy.deepcopy(factor2[factor2['ruas']==minruas])
        factor2 = factor2[factor2['ruas']!=minruas]
        factor2.loc[:,'ruas']=factor2.loc[:,'ruas']+maxruas
        middleport = pd.merge(keep1[['port','ruas','type','out']],keep2[['port','type','in','onboard','coverage']],
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


# %%
def calculate_fitness(ship_input, Population, df_port, df_portdistance, df_ship, df_rev, df_lowpeak, df_rulecost, 
                    df_cargoflat, df_pricecargoconfig, df_basefare, df_priceconfig, df_adjustment, season='LOW'):

    scored_dist=[]
    scored_tot=[]
    scored_cov=[]
    scored_profit=[]
    # if season=='PEAK':
    #     dot = str(df_lowpeak.loc[df_lowpeak.type=='PEAK','startdate'].iloc[0])
    # else:
    #     dot = str(df_lowpeak.loc[df_lowpeak.type=='LOW','startdate'].iloc[0])    

    for j,row in ship_input.iterrows():
        score_dist = []
        score_tot =[]
        score_cov = []
        score_profit = []
        idship = row.id
        
        # if ship_input.roundTrip[j]==1:
        #     maxdays = math.ceil(ship_input.maxComDays[j]/2)
        # else:
        maxdays = row.maxComDays
        
        mv = row.mustVisitPort
        mustvisit = port_mustvisit(mv,df_port)

        for i in range(0,len(Population)):
            gen = copy.deepcopy(Population[i][j])         
            if row.roundTrip==1:
                backtrip = copy.deepcopy(list(reversed(gen))[1:])
                [gen.insert(len(gen),i) for i in backtrip]
                
            hardfit = hard_fitness(idship, gen, maxdays, mustvisit, df_port, df_ship, df_portdistance)
            if hardfit==True:
                idgen = copy.deepcopy(gen)
                comdays = commission_days(idship,idgen,df_ship, df_port, df_portdistance)
                distance = int(calculate_totaldistance(idgen, df_portdistance).total_nautical[0])

                if comdays==0:
                    score_dist.append(0)
                    score_tot.append(0)
                    score_cov.append(0)
                    score_profit.append(0)
                else:
                    prediction = pd.DataFrame(calculate_prediction(idgen, df_rev))
                    cd_all = covered_demand(prediction, season)
                
                    a,movement = cal_factor(idship, idgen, cd_all, df_ship)
                    revenue,matrix = cal_revenue(idgen, None, movement, df_cargoflat, df_pricecargoconfig, df_lowpeak, df_basefare,
                                        df_priceconfig, df_adjustment,df_portdistance, season)
                    if len(revenue)>0:
                        rev = revenue['revenue'].sum()
                    else:
                        rev=0
                    
                    npax = movement.loc[movement['type']=='PASSENGER', 'total'].sum()
            
                    costdet = calculate_routecost(idship, idgen, npax, df_rulecost, df_ship, df_port, df_portdistance)
                    if len(costdet[0])>0:
                        cost = costdet[1].totalcost[0]
                    else:
                        cost = 0

                    profit = rev-cost

                    score_dist.append(distance/comdays)
                    score_profit.append(profit/comdays)

                    if len(a)==0:
                        coverage=0
                        totalin=0
                        score_tot.append(0)
                        score_cov.append(0)
                    else:
                        a = a.fillna(0)
                        a.loc[(a.coverage>10000),'coverage']=0
                        coverage = a['coverage'].mean()
                        totalin = a['in'].sum()
                        score_tot.append(totalin/comdays)
                        score_cov.append(coverage/comdays)
            else:
                score_dist.append(0)
                score_tot.append(0)
                score_cov.append(0)
                score_profit.append(0)
        scored_dist.append(score_dist)
        scored_tot.append(score_tot)
        scored_cov.append(score_cov)
        scored_profit.append(score_profit)
        
    #NORMALIZATION OF FITNESS SCORES
    norm_dist = [normalize(j) for j in scored_dist]
    norm_tot = [normalize(j) for j in scored_tot]
    norm_cov = [normalize(j) for j in scored_cov]
    norm_profit = [normalize(j) for j in scored_profit]
    
    return normalized_mean(ship_input, norm_dist, norm_tot, norm_cov, norm_profit), norm_profit

# %%
def new_cal_coverage(input, df_revenue, df_lowpeak, df_ship, season='LOW'):
    
    updated_demand = pd.DataFrame(columns=['origin', 'destination', 'type', 'total'])
    for i, data in enumerate(input):
        idship, idport_list = data
        capacity = ship_capacity(idship, df_ship)
        movement_df = covered_demand(idport_list, df_revenue, df_lowpeak, season)
        updated_demand = pd.concat([updated_demand, movement_df], ignore_index=True)
        coverage_df = cal_coverage(idship,idport_list, df_revenue, df_lowpeak, df_ship,season)
        condition_0 = (coverage_df['in'].notna()) | ((coverage_df['coverage'] > 0) & (coverage_df['coverage'] <= 1))  
        coverage_df = coverage_df[condition_0]
        coverage_over = coverage_df[coverage_df['coverage'] > 1]
        coverage_reg = coverage_df[coverage_df['coverage']<=1]

        for i, row in coverage_over.iterrows():
            uncovered = row['in'] - (row['onboard'] - capacity[row['type']])
            maxport = coverage_df['ruas'].max()
            while uncovered > 0 and maxport > 0:
                maxport_row = coverage_df[coverage_df['ruas'] == maxport]
                if not maxport_row.empty:
                    dest = maxport_row.iloc[0]['port']
                    org, des = row['port'], dest
                    od = updated_demand.loc[(updated_demand['origin'] == org) & (updated_demand['destination'] == des) & (updated_demand['type'] == row.type), 'total'] 
                    if len(od>0):
                        deducted = min(float(od),uncovered)
                        updated_demand.loc[(updated_demand['origin'] == org) & (updated_demand['destination'] == des), 'total'] = deducted
                        uncovered = uncovered - deducted
                    maxport = maxport - 1
                else:
                    break  

        updated_demand = pd.concat([updated_demand,coverage_over],ignore_index=True)

        temp_reg = []
        for i, row in coverage_reg.iterrows():
            try:
                ruas = int(row['ruas'])
            except ValueError:
                continue  
            
            source = row['port']
            while ruas <= coverage_reg['ruas'].max():
                dest_row = coverage_reg.loc[coverage_reg['ruas'] == ruas + 1]
                if not dest_row.empty:
                    dest = dest_row.iloc[0]['port']
                else:
                    dest = None
                type = row['type']
                total = 0
                temp_reg_data = {
                    'origin': source,
                    'destination': dest,
                    'type': type,
                    'total': total
                }
                temp_reg.append(temp_reg_data)
                ruas += 1

        new_coverage_reg = pd.DataFrame(temp_reg)

        updated_demand = pd.concat([updated_demand,new_coverage_reg],ignore_index=True)
        selected_columns = updated_demand[['origin', 'destination', 'type', 'total']].reset_index(drop=True)

    return selected_columns

# %%
def new_coverage_and_demand(input, season='LOW'):    
    updated_demand = pd.DataFrame(columns=['origin', 'destination', 'type', 'total'])
    for i, data in enumerate(input):
        idship, idport_list = data
        capacity = ship_capacity(idship)
        movement_df = covered_demand(idport_list, season)
        updated_demand = pd.concat([updated_demand, movement_df], ignore_index=True)
        coverage_df = cal_coverage(idship,idport_list,season)
        if len(coverage_df)>0:
            condition_0 = (coverage_df['in'].notna()) | ((coverage_df['coverage'] > 0) & (coverage_df['coverage'] <= 1))  
            coverage_df = coverage_df[condition_0]
            coverage_df = coverage_df.fillna(0)
            coverage_over = coverage_df[coverage_df['coverage'] > 1]
            coverage_reg = coverage_df[coverage_df['coverage']<=1]

            for i, row in coverage_over.iterrows():
                if row.coverage<99999:
                    ruas = row.ruas
                    #rb = ruas-1
                    before = (coverage_df.ruas<ruas)&(coverage_df.type==row.type)
                    rowb = coverage_df[before]
                    if len(rowb)>0:
                        rowb = rowb[rowb.ruas==max(rowb.ruas)]
                        rowb = rowb.iloc[0]
                        new_onboard = rowb['onboard']-row['out']+row['in']
                        new_in = row['in']
                        uncovered = 0
                        if new_onboard>capacity[row.type]:
                            new_onboard = capacity[row.type]
                            new_in = min((capacity[row.type]-(rowb.onboard-row.out)), capacity[row.type])
                            uncovered = row['in']-new_in
                    else:
                        new_onboard = capacity[row.type]
                        new_in = capacity[row.type]
                        uncovered = row['in']-new_in
                        
                    new_out = min(row.out,capacity[row.type])
                    this = (coverage_df.ruas==row.ruas)&(coverage_df.type==row.type)&(coverage_df.port==row.port)
                    coverage_df.loc[this,'in']=new_in
                    coverage_df.loc[this,'out']=new_out
                    coverage_df.loc[this,'onboard']=new_onboard
                    coverage_df.loc[this,'coverage']=new_onboard/capacity[row.type]
                    
                    maxport = coverage_df['ruas'].max()
                    while uncovered > 0 and maxport > 0:
                        maxport_row = coverage_df[coverage_df['ruas'] == maxport]
                        if not maxport_row.empty:
                            dest = maxport_row.iloc[0]['port']
                            org, des = row['port'], dest
                            od_idx = (updated_demand['origin'] == org) & (updated_demand['destination'] == des) & (updated_demand['type'] == row.type)
                            od = updated_demand.loc[od_idx, 'total'] 
                            if len(od>0):
                                deducted = min(float(od.iloc[0]),uncovered)
                                updated_demand.loc[od_idx, 'total'] = deducted
                                uncovered = uncovered - deducted
                            maxport = maxport - 1
                        else:
                            break  

            updated_demand = pd.concat([updated_demand,coverage_over],ignore_index=True)

            temp_reg = []
            for i, row in coverage_reg.iterrows():
                try:
                    ruas = int(row['ruas'])
                except ValueError:
                    continue  
                
                source = row['port']
                while ruas <= coverage_reg['ruas'].max():
                    dest_row = coverage_reg.loc[coverage_reg['ruas'] == ruas + 1]
                    if not dest_row.empty:
                        dest = dest_row.iloc[0]['port']
                    else:
                        dest = None
                    type = row['type']
                    total = 0
                    temp_reg_data = {
                        'origin': source,
                        'destination': dest,
                        'type': type,
                        'total': total
                    }
                    temp_reg.append(temp_reg_data)
                    ruas += 1

            new_coverage_reg = pd.DataFrame(temp_reg)

            updated_demand = pd.concat([updated_demand,new_coverage_reg],ignore_index=True)
            selected_columns = updated_demand[['origin', 'destination', 'type', 'total']].reset_index(drop=True)
        else:
            selected_columns = coverage_df
        
        return coverage_df, selected_columns

# %%


# %%
def make_sorter(l):
    sort_order = {k:v for k, v in zip(l, range(len(l)))}
    return lambda s: s.map(lambda x:sort_order[x])

# %%
def calculate_distanceperport_with_permutation(portlist, df_portdistance): 
    origin = []
    destination = []
    size = len(portlist) - 1

    for i in range(size):
        a = portlist[i]
        for j in range (size - i):
            idx = i + j + 1
            b = portlist[idx]
            origin.append(a)
            destination.append(b)

    # od_pair = pd.DataFrame({'id_origin' : origin, 'id_destination' : destination})
    od_pair = pd.DataFrame((itertools.permutations(portlist, 2)), columns=['id_origin', 'id_destination'])
    
    df_join = pd.merge(df_portdistance, od_pair, how='inner', left_on=('id_origin', 'id_destination'), 
                       right_on=('id_origin', 'id_destination')).drop(columns = ['commercial'])
    return df_join

# %%
def port_arranger_by_distance(portlist, df_portdistance):
    # ori = copy.deepcopy(portlist[0])
    # des = copy.deepcopy(portlist[-1])
    # portlist = copy.deepcopy(portlist[1:-1])
    distanceperport = calculate_distanceperport_with_permutation(portlist, df_portdistance)
    sorter_list = copy.deepcopy(portlist)
    distanceperport = distanceperport.sort_values(by='id_origin', key=make_sorter(sorter_list)).reset_index(drop=True)
    # display(distanceperport)
    arranged_port = pd.DataFrame(columns=['origin', 'destination', 'nautical'])
    for i in range(len(portlist) - 1):
        # first mile
        if i == 0: 
            row_selection = copy.deepcopy(distanceperport[distanceperport['id_origin'] == portlist[i]].sort_values(by='nautical', ascending=True).reset_index(drop=True))
            if len(row_selection)>0:
                origin = row_selection['id_origin'].iloc[0]
                destination = row_selection['id_destination'].iloc[0]
                nautical = row_selection['nautical'].iloc[0]
                add_port = pd.DataFrame({'origin' : origin, 'destination' : destination ,'nautical' : nautical}, index=[i])
                arranged_port = pd.concat([arranged_port, add_port])
        else:
            row_selection = copy.deepcopy(distanceperport[(distanceperport['id_origin'] == arranged_port.loc[(i-1), 'destination']) & 
                                            (distanceperport['id_destination'] != arranged_port.loc[(i-1), 'origin'])].sort_values(by='nautical').reset_index(drop=True))
            # row_selection = row_selection.loc[1:, :].reset_index(drop=True)

            row_selection = row_selection[~row_selection['id_destination'].isin(arranged_port['origin'])]
            if len(row_selection)>0:
                origin = row_selection['id_origin'].iloc[0]
                destination = row_selection['id_destination'].iloc[0]
                nautical = row_selection['nautical'].iloc[0]
                add_port = pd.DataFrame({'origin' : origin, 'destination' : destination ,'nautical' : nautical}, index=[i])
                arranged_port = pd.concat([arranged_port, add_port])

    arranged_port = arranged_port.reset_index(drop=True)
    list_of_port_arranged = copy.deepcopy(arranged_port['origin'].tolist())
    list_of_port_arranged.append(arranged_port['destination'].iloc[-1])
    
    # list_of_port_arranged.insert(0,ori)
    # list_of_port_arranged.append(des)

    return list_of_port_arranged

# %%
def eligible_port(id_ship, df_port, df_ship):
    #global df_ship,df_port
    
    df_ship1 = copy.deepcopy(df_ship[['id','length','draft']])   
    df_ship1 = df_ship1[df_ship1['id'] == id_ship]
    df_ship1 = df_ship1.fillna(0)    
    
    df_port1 = copy.deepcopy(df_port[['id','length','depth']]) 
    df_port1 = df_port1.fillna(0)
    df_port1.loc[df_port1['length']=='', 'length']=0
    df_port1.loc[df_port1['depth']=='', 'depth']=0  
    
    # Extract values from df_ship dataframe
    length_ship = df_ship1['length'].iloc[0]
    draft = df_ship1['draft'].iloc[0]

    df_port2=df_port1[df_port1['length'].astype(float)>=length_ship]
    df_port3=df_port2[df_port2['depth'].astype(float)>=draft]
    output = list(df_port3.id)
    return output

# %%
def fix_fitness(idship,gen,maxdays,minp,mustvisit,ori,des,listport,df_port,df_ship,df_portdistance):
    hardfit=hard_fitness(idship,gen,maxdays,mustvisit, df_port, df_ship, df_portdistance)
    iter=0
    nport = len(gen)
    np = min((nport-3),(len(listport)-1))
    gen_sort = copy.deepcopy(gen)
    while hardfit==False and iter<20:
        shuffle(listport)
        shuffle(mustvisit)
        rrr=listport[0:np]
        gen[2:(nport-1)]=rrr
        gen = fix_mustvisit(mustvisit,gen,ori,des)
        gen_sort = copy.deepcopy(gen)
        if len(gen_sort)>2:
            arranged_port = copy.deepcopy(gen_sort)
            if des in arranged_port:
                arranged_port.remove(des)
            if len(unique(arranged_port))>1:
                gen_sort = copy.deepcopy(port_arranger_by_distance(list(unique(arranged_port)), df_portdistance))
                gen_sort.append(des)
        gen = copy.deepcopy(gen_sort)
        hardfit=hard_fitness(idship,gen,maxdays,mustvisit,df_port, df_ship, df_portdistance)
        iter=iter+1
    if hardfit==False:
        iter2=0
        while (hardfit==False) and (len(gen)>minp) and (iter2<30):
            gen.remove(gen[-2])
            gen = fix_mustvisit(mustvisit,gen,ori,des)
            gen_sort = copy.deepcopy(gen)
            if len(gen_sort)>2:
                arranged_port = copy.deepcopy(gen_sort)
                if des in arranged_port:
                    arranged_port.remove(des)
                if len(unique(arranged_port))>1:
                    gen_sort = copy.deepcopy(port_arranger_by_distance(list(unique(arranged_port)), df_portdistance))
                    # gen_sort.insert(0, ori)
                    gen_sort.append(des)
            gen = copy.deepcopy(gen_sort)
            hardfit=hard_fitness(idship,gen,maxdays,mustvisit,df_port, df_ship, df_portdistance)
            iter2=+1
    return gen_sort

# %%
def generate_genome(ship_input, df_port, df_ship, df_portdistance):
    genome = []
    for i,row in ship_input.iterrows():
        ori = row.originPort
        des = row.destinationPort
        mv = row.mustVisitPort
        mustvisit = port_mustvisit(mv,df_port)
        mustminport = len(unique([ori,des,*mustvisit]))
        idship=row.id
        listport=eligible_port(idship, df_port, df_ship)
        
        if row.roundTrip==1:
            minp = max(math.ceil(int(row.minPort)/2),mustminport)
            if minp<=2:
                minp=3
            maxdays = max(int(row.minComDays),math.ceil(int(row.maxComDays)/2))
            maxp = max(minp,math.floor(int(row.maxPort)/2))
        else:
            minp = max(int(row.minPort),mustminport)
            maxdays = row.maxComDays
            maxp = max(minp,int(row.maxPort))
        
        if minp!=maxp:
            nport = choices(range(minp,maxp),k=1)[0]
        else:
            nport = minp

        gen=choices(['-1'],k=nport)

        home = row.homeBase

        if ori!='':
            gen[0]=ori
            if ori in listport: 
                listport.remove(ori)
            #ports.remove(ori)
            if (ori!=home) & (home!=''):
                gen[1]=home
                if home in listport: 
                    listport.remove(home)
                #ports.remove(home)
            else:
                r=choices(listport,k=1)[0]
                gen[1]=r
            #ports.remove(r)
        else:
            if home!='':
                gen[0]=home
                #ports.remove(home)
            else:
                r=choices(listport,k=1)[0]
                gen[0]=r
                #ports.remove(r)
            r=choices(listport,k=1)[0]
            gen[1]=r
            #ports.remove(r)
        if des!='':
            gen[-1]=des
            if des in listport: 
                listport.remove(des)
            #ports.remove(des)
        else:
            shuffle(listport)
            gen[-1]=listport[0]
            listport.remove(listport[0])
            #ports.remove(ports[0])
        
        if '-1' in gen:
            np = min((nport-3),(len(listport)-1))
            rrr=listport[0:np]
            gen[2:(nport-1)]=rrr
            gen = fix_mustvisit(mustvisit,gen,ori,des)
            gen_sort = fix_fitness(idship,gen,maxdays,minp,mustvisit,ori,des,listport,df_port,df_ship,df_portdistance)
            
        else:
            gen = fix_mustvisit(mustvisit,gen,ori,des)
        gen_sort = copy.deepcopy(unique(gen))
        if len(gen_sort)>2:
            arranged_port = copy.deepcopy(gen_sort)
            if des in arranged_port:
                arranged_port.remove(des)
            if len(unique(arranged_port))>2:
                gen_sort = copy.deepcopy(port_arranger_by_distance(list(unique(arranged_port)), df_portdistance))
                # gen_sort.insert(0, ori)
                gen_sort.append(des)
            gen_sort = fix_fitness(idship,gen_sort,maxdays,minp,mustvisit,ori,des,listport,df_port,df_ship,df_portdistance)
        
        if '' in gen_sort:
            gen_sort.remove('') 
            if '' in gen_sort: 
                gen_sort.remove('')
        genome.append(gen_sort)
        
    return genome

# %%
def ship_port_relation(id_port,id_ship, df_port, df_ship):
    #global df_port, df_ship

    df_ship1 = df_ship[['id','length','draft']]    
    df_ship1 = df_ship1[df_ship1['id'] == id_ship]
    df_ship1 = df_ship1.fillna(0)    
    
    df_port1 = df_port[['id','length','depth']]
    df_port1 = df_port1[df_port1['id'].isin(id_port)]
    df_port1 = df_port1.fillna(0)
    
    # Extract values from df_ship dataframe
    length_ship = df_ship1['length'].iloc[0]
    draft = df_ship1['draft'].iloc[0]

    # Check the first condition: length_ship <= length_port
    output = []
    for i,dfp in df_port1.iterrows():
        if length_ship <= int(dfp.length):
            # Check the second condition: draft <= depth
            if draft <= int(dfp.depth):
                output.append(1)
            else:
                output.append(0)
        else:
            output.append(0)
    
    return output

# %%
def selection_topship_a(ship_input, Population, norm_mean, n):
    topgen =[]
    for i in range(0,len(ship_input)):
        t_value = sorted(norm_mean[i])[-n:]
        t_index = [norm_mean[i].index(t) for t in t_value]
        t_gen = [copy.deepcopy(Population[g][i]) for g in t_index]
        topgen.append(t_gen)
    topgen = [list(x) for x in zip(*topgen)]
    topgen = list(reversed(topgen))
    # lowgen = topgen[n:]
    # topgen = topgen[0:n]
    return topgen #,lowgen

# %%
def selection_topship(ship_input, Population, norm_mean, n):
    topgen =[]
    for i in range(0,len(ship_input)):
        sortmean = sorted(unique(norm_mean[i]))
        t_value = sortmean[-n:]
        t_index = [norm_mean[i].index(t) for t in t_value]
        t_gen = [copy.deepcopy(Population[g][i]) for g in t_index]
        topgen.append(t_gen)
    topgen = [list(x) for x in zip(*topgen)]
    topgen = list(reversed(topgen))
    # lowgen = topgen[n:]
    # topgen = topgen[0:n]
    return topgen #,lowgen

# %%
def single_point_crossover(a, b):
    if len(a)!=len(b):
        raise ValueError("Genomes a and b must be of same length")
        
    length = len(a)
    if length<2:
        return a,b
    
    p =randint(1, length-1)
    return [a[0:p]+b[p:], b[0:p]+a[p:]]

# %%
def output_dataframes_old(ship_input,next_generation,df_port, df_ship, df_portdistance, df_adjustment,
                            df_basefare, df_cargoflat, df_lowpeak, df_pricecargoconfig, 
                            df_priceconfig, df_rev, df_rulecost, season ='LOW'):    

    est_route_detail = pd.DataFrame(columns=['idconfdetail','option','type', 'origin', 'total', 'revenue', 'port', 'ruas',
                                    'in', 'out', 'onboard', 'coverage'])
    matrix_detail = pd.DataFrame(columns=['idconfdetail','option','typeSeason','origin','destination','type','total'])
    
    for k in range(len(next_generation)): 
        if k==0:
            fillOption = 'ONE'
        elif k==1:
            fillOption = 'TWO'
        else:
            fillOption = 'THREE'
        for i in range(len(ship_input)):
            factor = pd.DataFrame(columns=['port','ruas','type','in','out','onboard','coverage'])
            revenue = pd.DataFrame(columns=['type','origin','ruas','total','revenue'])
            idconfdet = ship_input.idConfRouteDetail[i]
            gen = copy.deepcopy(next_generation[k][i])
            if ship_input.roundTrip[i]==1:
                backtrip = copy.deepcopy(list(reversed(gen))[1:])
                [gen.insert(len(gen),i) for i in backtrip]
            ports = copy.deepcopy(gen)
            
            prediction = pd.DataFrame(calculate_prediction(ports, df_rev))
            cd_all = covered_demand(prediction, season)
            cov_det,movement = cal_factor(ship_input.id[i], ports, cd_all, df_ship)
            
            if len(cov_det)>0:
                factor = pd.concat([factor,cov_det])
            
            factor['idconfdetail']= idconfdet 
            factor['option']=fillOption
            
            rev_det,matrix = cal_revenue(ports, None, movement, df_cargoflat, df_pricecargoconfig, df_lowpeak, df_basefare,
                                 df_priceconfig, df_adjustment, df_portdistance, season)
            matrix['idconfdetail']= idconfdet
            matrix['option']=fillOption

            if len(rev_det)>0:
                revenue = pd.concat([revenue,rev_det])
            if (len(revenue)>0) & (len(factor)>0) & (sum(revenue['revenue'])>0):
                # df_join = pd.merge(revenue, factor, how='inner', left_on=('type','origin','ruas','voyage'), 
                #                 right_on=('type','port','ruas','voyage')).drop(columns=['origin'])
                merged1 = pd.merge(revenue,factor, how='right', left_on=['origin','type','ruas'], right_on=['port','type','ruas'])
                maxruas = copy.deepcopy(max(merged1['ruas']))
                merged1 = merged1.drop(list(merged1.loc[merged1['ruas'] == maxruas].index))
                merged2 = pd.merge(factor,revenue, how='outer', left_on=['port','ruas','type'], right_on=['origin','ruas','type']).drop(columns = ['origin'])
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
                matrix_detail = pd.concat([matrix_detail,matrix])
                est_route_detail = pd.concat([est_route_detail,filtered])
            
    #est_route_detail = est_route_detail.drop(['origin','total'])
    est_route_detail = est_route_detail.rename(columns={'idconfdetail' : 'idConfDetail',
                                                        'port' : 'portOrigin',
                                                        'revenue': 'estRevenue',
                                                        'in':'estUp',
                                                        'out':'estDown',
                                                        'onboard':'estOnboard',
                                                        'coverage':'factor' 
                                                         })
    
    matrix_detail = matrix_detail.rename(columns={'idconfdetail' : 'idConfDetail',
                                                        'origin' : 'portOrigin',
                                                        'destination': 'portDestination'
                                                         })

    if season=='LOW':
        fillSeason = 'REGULAR'
    else:
        fillSeason = 'PEAK'

    est_route_detail['typeSeason'] = fillSeason
    matrix_detail['typeSeason'] = fillSeason

    header_all = pd.DataFrame(columns=['idConfDetail','option','typeSeason','commision','totSubcost','avgFactor','totDistance',
                                   'avgOnboard','totTotal','totRevenue'])
    cost_all = pd.DataFrame(columns=['idConfDetail','option','typeSeason','name', 'npax', 'sailingtime', 'berthingtime', 'idship', 'idport',
                                 'time', 'expenseperday', 'cost'])

    confdet = list(ship_input.idConfRouteDetail)
    for cd in confdet:
        idship=ship_input[ship_input.idConfRouteDetail==cd]['id'].iloc[0]
        options = est_route_detail.loc[est_route_detail['idConfDetail']==cd,'option'].unique()
        for op in options:
            est_opt = est_route_detail[(est_route_detail['option']==op) & (est_route_detail['idConfDetail']==cd)
                                        &(est_route_detail['typeSeason']==fillSeason)]
            est_opt = est_opt.sort_values(by='ruas', ascending=True)
            # listport = list(est_opt['portOrigin'].unique())
            listport = unique(est_opt.sort_values(by="ruas")['portOrigin'])
            gen = copy.deepcopy(listport)
            rtrip = ship_input[ship_input.idConfRouteDetail==cd]['roundTrip'].iloc[0] 
            if rtrip==1:
                backtrip = copy.deepcopy(list(reversed(gen))[1:])
                [gen.insert(len(gen),i) for i in backtrip]
            ports = copy.deepcopy(gen)
            
            prediction = pd.DataFrame(calculate_prediction(ports, df_rev))
            cd_all = covered_demand(prediction, season)
            npax = cd_all.loc[cd_all['type']=='PASSENGER', 'total'].sum()
                
            cal_cost = calculate_routecost(idship, ports, npax, df_rulecost, df_ship, df_port, df_portdistance)
            cost_det = cal_cost[0]
            cost_det['idConfDetail'] = cd
            cost_det['option'] = op
            cost_det['typeSeason'] = fillSeason
            if len(cost_det)>0:
                cost_tot = cal_cost[1]
                cost_all = pd.concat([cost_all,cost_det], axis=0)
            else:
                cost_tot = 0

            header_det = pd.DataFrame(columns=['idConfDetail','option','typeSeason','totSubcost','avgFactor','totDistance',
                                               'avgOnboard','totTotal','totRevenue','totSail','totBerth'])
            # comm =  commission_days(idship,ports,df_ship,df_port,df_portdistance)
            timecal = pd.DataFrame(calculate_time(idship, ports, df_ship, df_port, df_portdistance))
            timecal.set_index('Name', inplace=True)
            sailing_time = timecal.loc['Sailing Time']['Amount']
            berthing_time = timecal.loc['Berthing Time']['Amount']
            comm = timecal.loc['Commission Days']['Amount']

            header_det.loc[0,'idConfDetail'] = cd
            header_det['option'] = op
            header_det['typeSeason'] = fillSeason
            header_det['commision'] = comm
            header_det['totSail'] = sailing_time
            header_det['totBerth'] = berthing_time
            header_det['totSubcost'] = cost_tot
            estfactor = est_opt['factor'].mean()
            if estfactor>9999:
                estfactor = 99999
            header_det['avgFactor'] = round(estfactor,2) 
            header_det['totDistance'] = calculate_totaldistance(ports, df_portdistance).total_nautical.iloc[0]
            header_det['avgOnboard'] = round(est_opt['estOnboard'].mean(),2)
            header_det['totTotal'] = round(est_opt['total'].sum(),2)
            header_det['totRevenue'] = round(est_opt['estRevenue'].sum(),2)
            header_all = pd.concat([header_all,header_det], axis=0, ignore_index=True)
    
    return header_all, est_route_detail, cost_all, matrix_detail

# %%
def output_dataframes(ship_input,next_generation,df_port, df_ship, df_portdistance, df_adjustment,
                            df_basefare, df_cargoflat, df_lowpeak, df_pricecargoconfig, 
                            df_priceconfig, df_rev, df_rulecost, season ='LOW'):    

    est_route_detail = pd.DataFrame(columns=['idconfdetail','option','type', 'origin', 'total', 'revenue', 'port', 'ruas',
                                    'in', 'out', 'onboard', 'coverage'])
    matrix_detail = pd.DataFrame(columns=['idconfdetail','option','typeSeason','origin','destination','type','total'])
        
    header_all = pd.DataFrame(columns=['idConfDetail','option','typeSeason','commision','totSubcost','avgFactor','totDistance',
                                   'avgOnboard','totTotal','totRevenue'])
    cost_all = pd.DataFrame(columns=['idConfDetail','option','typeSeason','name', 'npax', 'sailingtime', 'berthingtime', 'idship', 'idport',
                                 'time', 'expenseperday', 'cost'])

    for k in range(len(next_generation)): 
        if k==0:
            fillOption = 'ONE'
        elif k==1:
            fillOption = 'TWO'
        else:
            fillOption = 'THREE'
        for i in range(len(ship_input)):
            factor = pd.DataFrame(columns=['port','ruas','type','in','out','onboard','coverage'])
            revenue = pd.DataFrame(columns=['type','origin','ruas','total','revenue'])
            idconfdet = ship_input.idConfRouteDetail[i]
            gen = copy.deepcopy(next_generation[k][i])
            if ship_input.roundTrip[i]==1:
                backtrip = copy.deepcopy(list(reversed(gen))[1:])
                [gen.insert(len(gen),i) for i in backtrip]
            ports = copy.deepcopy(gen)
            idship = ship_input.id[i]
            prediction = pd.DataFrame(calculate_prediction(ports, df_rev))
            cd_all = covered_demand(prediction, season)
            cov_det,movement = cal_factor(idship, ports, cd_all, df_ship)
            
            if len(cov_det)>0:
                factor = pd.concat([factor,cov_det])
            
            factor['idconfdetail']= idconfdet 
            factor['option']=fillOption
            
            rev_det,matrix = cal_revenue(ports, None, movement, df_cargoflat, df_pricecargoconfig, df_lowpeak, df_basefare,
                                 df_priceconfig, df_adjustment, df_portdistance, season)
            matrix['idconfdetail']= idconfdet
            matrix['option']=fillOption

            if len(rev_det)>0:
                revenue = pd.concat([revenue,rev_det])
            if (len(revenue)>0) & (len(factor)>0) :
                # df_join = pd.merge(revenue, factor, how='inner', left_on=('type','origin','ruas','voyage'), 
                #                 right_on=('type','port','ruas','voyage')).drop(columns=['origin'])
                merged1 = pd.merge(revenue,factor, how='right', left_on=['origin','type','ruas'], right_on=['port','type','ruas'])
                maxruas = copy.deepcopy(max(merged1['ruas']))
                merged1 = merged1.drop(list(merged1.loc[merged1['ruas'] == maxruas].index))
                merged2 = pd.merge(factor,revenue, how='outer', left_on=['port','ruas','type'], right_on=['origin','ruas','type']).drop(columns = ['origin'])
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
                matrix_detail = pd.concat([matrix_detail,matrix])
                est_route_detail = pd.concat([est_route_detail,filtered])

            # header_all['idconfdetail']= idconfdet
            # header_all['option']=fillOption

            # cost_all['idconfdetail']= idconfdet
            # cost_all['option']=fillOption
            est_route = est_route_detail[(est_route_detail['idconfdetail']==idconfdet)&(est_route_detail['option']==fillOption)]
            npax = movement.loc[movement['type']=='PASSENGER', 'total'].sum()
            
            cal_cost = calculate_routecost(idship, ports, npax, df_rulecost, df_ship, df_port, df_portdistance)
            cost_det = cal_cost[0]
            cost_det['idConfDetail'] = idconfdet
            cost_det['option'] = fillOption
            # cost_det['typeSeason'] = fillSeason
            if len(cost_det)>0:
                cost_tot = cal_cost[1]
                cost_all = pd.concat([cost_all,cost_det], axis=0)
            else:
                cost_tot = 0

            header_det = pd.DataFrame(columns=['idConfDetail','option','typeSeason','totSubcost','avgFactor','totDistance',
                                               'avgOnboard','totTotal','totRevenue','totSail','totBerth'])
            # comm =  commission_days(idship,ports,df_ship,df_port,df_portdistance)
            timecal = pd.DataFrame(calculate_time(idship, ports, df_ship, df_port, df_portdistance))
            timecal.set_index('Name', inplace=True)
            sailing_time = timecal.loc['Sailing Time']['Amount']
            berthing_time = timecal.loc['Berthing Time']['Amount']
            comm = timecal.loc['Commission Days']['Amount']

            header_det.loc[0,'idConfDetail'] = idconfdet
            header_det['option'] = fillOption
            # header_det['typeSeason'] = fillSeason
            header_det['commision'] = comm
            header_det['totSail'] = sailing_time
            header_det['totBerth'] = berthing_time
            header_det['totSubcost'] = cost_tot
            estfactor = est_route['coverage'].mean()
            if estfactor>9999:
                estfactor = 99999
            header_det['avgFactor'] = round(estfactor,2)
            header_det['totDistance'] = calculate_totaldistance(ports, df_portdistance).total_nautical.iloc[0]
            header_det['avgOnboard'] = round(est_route['onboard'].mean(),2)
            header_det['totTotal'] = round(est_route['in'].sum(),2)
            header_det['totRevenue'] = round(est_route['revenue'].sum(),2)
            header_all = pd.concat([header_all,header_det], axis=0, ignore_index=True)

    #est_route_detail = est_route_detail.drop(['origin','total'])
    est_route_detail = est_route_detail.rename(columns={'idconfdetail' : 'idConfDetail',
                                                        'port' : 'portOrigin',
                                                        'revenue': 'estRevenue',
                                                        'in':'estUp',
                                                        'out':'estDown',
                                                        'onboard':'estOnboard',
                                                        'coverage':'factor' 
                                                         })
    
    matrix_detail = matrix_detail.rename(columns={'idconfdetail' : 'idConfDetail',
                                                        'origin' : 'portOrigin',
                                                        'destination': 'portDestination'
                                                         })

    if season=='LOW':
        fillSeason = 'REGULAR'
    else:
        fillSeason = 'PEAK'

    est_route_detail['typeSeason'] = fillSeason
    matrix_detail['typeSeason'] = fillSeason
    header_all['typeSeason'] = fillSeason
    cost_all['typeSeason'] = fillSeason

    return header_all, est_route_detail, cost_all, matrix_detail

# %%
def generate_route_alpha_ver(ship_input, df_port, df_ship, df_portdistance, df_adjustment,
                            df_basefare, df_cargoflat, df_lowpeak, df_pricecargoconfig, 
                            df_priceconfig, df_revenue, df_rulecost, season='LOW', npop=20): #input jumlah iterasi
    #global df_port, df_lowpeak

    ## GENERATE INITIAL POPULATION
    #season='PEAK'
    #npop=20
    start = time.time()
    ports = list(df_port.id)
    Population = []
    for i in range(npop):
        Genome=generate_genome(ship_input, ports, df_port, df_ship, df_portdistance)
        Population.append(Genome)

    end = time.time()        
    print(f"time spent for population-generation: {end-start}s")
    
    ## CALCULATE FITNESS SCORES
    start = time.time()

    #fitness ship
    norm_mean = calculate_fitness(ship_input, Population, df_port, df_portdistance, df_ship, df_revenue, df_lowpeak, df_rulecost, 
                    df_cargoflat, df_pricecargoconfig, df_basefare, df_priceconfig, df_adjustment, season)

    #fitness route
    #itung score per port new_cal_coverage, cost, revenue...

    end = time.time()        
    print(f"time spent for score-fitting: {end-start}s")
    
    ### POPULATION SELECTION 
    next_generation = []

    ## SELECTION PROCESS 1 -Elitism of each ship
    next_generation += selection_topship(ship_input, Population, norm_mean, 3)
    
    dfs = output_dataframes(ship_input,next_generation,df_port, df_ship, df_portdistance, df_adjustment,
                            df_basefare, df_cargoflat, df_lowpeak, df_pricecargoconfig, 
                            df_priceconfig, df_revenue, df_rulecost, season)
    header_all = dfs[0]
    est_route_detail = dfs[1]
    cost_all = dfs[2]  
    
    return header_all, est_route_detail, cost_all

# %%
def selection_random(population, fitscore):
    return choices(
        population=population,
        weights=fitscore,
        k=2
    )

# %%
def mutation(genome, ship_input, df_port, df_ship, df_portdistance, num=1,probability=0.5):
    
    for j,row in ship_input.iterrows():
        
        # row = ship_input.iloc[j]
        listport=eligible_port(row.id, df_port, df_ship)
        gen = copy.deepcopy(genome[j])
        
        gen_sort = copy.deepcopy(gen)
        idship = row.id
        
        ori = row.originPort
        des = row.destinationPort
        mv = row.mustVisitPort
        mustvisit = port_mustvisit(mv,df_port)
        mustminport = len(unique([ori,des,*mustvisit]))

        if row.roundTrip==1:
            minp = max(math.ceil(int(row.minPort)/2),mustminport)
            if minp<=2:
                minp=3
            maxdays = max(int(row.minComDays),math.ceil(int(row.maxComDays)/2))
        else:
            minp = max(int(row.minPort),mustminport)
            maxdays = row.maxComDays
        
        if(len(gen)>2):
            for _ in range(num):
                if random() > probability:
                    options = copy.deepcopy(listport)
                    [options.remove(x) for x in gen if x in options]
                    if len(options)>0:
                        indexg = randrange(1,(len(gen)-1))
                        indexo = randrange(0,len(options))
                        gen[indexg] = options[indexo]
                        gen = copy.deepcopy(fix_mustvisit(mustvisit,gen,ori,des))
                        gen_sort = copy.deepcopy(gen)
                        if len(gen_sort)>2:
                            arranged_port = copy.deepcopy(gen_sort)
                            if des in arranged_port:
                                arranged_port.remove(des)
                            if len(unique(arranged_port))>1:
                                gen_sort = copy.deepcopy(port_arranger_by_distance(list(unique(arranged_port)), df_portdistance))
                                gen_sort.append(des)
                        if '' in gen_sort:
                            gen_sort.remove('')   
                        
                        if len(gen_sort)>len(unique(gen_sort)):
                            print('A1 CAUTION')   
        
                        hardfit=hard_fitness(idship,gen_sort,maxdays,mustvisit, df_port, df_ship, df_portdistance)
                        iter=0
                        nport = len(gen_sort)
                        np = min((nport-3),(len(options)-1))
                        while hardfit==False and iter<20:
                            shuffle(options)
                            shuffle(mustvisit)
                            [options.remove(x) for x in gen_sort if x in options]
                            rrr=options[0:np]
                            # print(f'rrr: {rrr}')
                            # print(f'genCCC_0: {gen_sort}')
                            gen_sort[2:-1]=rrr
                            # print(f'genCCC_1: {gen_sort}')
                            gen_sort = fix_mustvisit(mustvisit,gen_sort,ori,des)
                            if len(gen_sort)>len(unique(gen_sort)):
                                print('A2 CAUTION')
                                
                            if len(gen_sort)>2:
                                arranged_port = copy.deepcopy(gen_sort)
                                if des in arranged_port:
                                    arranged_port.remove(des)
                                if len(unique(arranged_port))>1:
                                    gen_sort = copy.deepcopy(port_arranger_by_distance(list(unique(arranged_port)), df_portdistance))
                                    gen_sort.append(des)
                                    if '' in gen_sort:
                                        gen_sort.remove('')
                            
                            if len(gen_sort)>len(unique(gen_sort)):
                                print('A3 CAUTION')  

                            hardfit=hard_fitness(row.id,gen_sort,maxdays,mustvisit,df_port, df_ship, df_portdistance)
                            iter=iter+1
                        if hardfit==False:
                            iter2=0
                            while (hardfit==False) and (len(gen_sort)>minp) and (iter2<20):
                                gen_sort.remove(gen_sort[-2])
                                gen_sort = fix_mustvisit(mustvisit,gen_sort,ori,des)
                                if len(gen_sort)>len(unique(gen_sort)):
                                    print('A4 CAUTION')

                                if len(gen_sort)>2:
                                    arranged_port = copy.deepcopy(gen_sort)
                                    if des in arranged_port:
                                        arranged_port.remove(des)
                                    if len(unique(arranged_port))>1:
                                        gen_sort = copy.deepcopy(port_arranger_by_distance(list(unique(arranged_port)), df_portdistance))
                                        gen_sort.append(des)
                                        if '' in gen_sort:
                                            gen_sort.remove('')

                                if len(gen_sort)>len(unique(gen_sort)):
                                    print('A5 CAUTION')

                                hardfit=hard_fitness(row.id,gen_sort,maxdays,mustvisit,df_port, df_ship, df_portdistance)
                                iter2=+1
        if '' in gen_sort:
            gen_sort.remove('') 
            if '' in gen_sort: 
                gen_sort.remove('')
        if len(gen_sort)>len(unique(gen_sort)):
            print('CAUTION!')
        genome[j] = gen_sort
    return genome

# %%
def mutation_old2(genome, ship_input, df_port, df_ship, df_portdistance, num=1,probability=0.5):
    for j in range(len(genome)):
        
        row = ship_input.loc[j]
        listport=eligible_port(ship_input.id[j], df_port, df_ship)
        mv = ship_input.mustVisitPort[j]
        mustvisit = port_mustvisit(mv,df_port)
        gen = copy.deepcopy(genome[j])
        
        gen_sort = copy.deepcopy(gen)
        idship = row.id
        
        ori = row.originPort
        des = row.destinationPort
        mv = row.mustVisitPort
        mustvisit = port_mustvisit(mv,df_port)
        mustminport = len(unique([ori,des,*mustvisit]))

        if row.roundTrip==1:
            minp = max(math.ceil(int(row.minPort)/2),mustminport)
            if minp<=2:
                minp=3
            maxdays = max(int(row.minComDays),math.ceil(int(row.maxComDays)/2))
        else:
            minp = max(int(row.minPort),mustminport)
            maxdays = row.maxComDays
        
        ori = ship_input.originPort[j]
        des = ship_input.destinationPort[j]
        if(len(gen)>2):
            for _ in range(num):
                if random() > probability:
                    options = copy.deepcopy(listport)
                    [options.remove(x) for x in gen if x in options]
                    if len(options)>0:
                        indexg = randrange(1,(len(gen)-1))
                        indexo = randrange(0,len(options))
                        gen[indexg] = options[indexo]
                        gen = copy.deepcopy(fix_mustvisit(mustvisit,gen,ori,des))
                        gen_sort = copy.deepcopy(gen)
                        if len(gen_sort)>2:
                            arranged_port = copy.deepcopy(gen_sort)
                            if des in arranged_port:
                                arranged_port.remove(des)
                            if len(unique(arranged_port))>1:
                                gen_sort = copy.deepcopy(port_arranger_by_distance(list(unique(arranged_port)), df_portdistance))
                                gen_sort.append(des)
                        if '' in gen_sort:
                            gen_sort.remove('')   
                        
                        if len(gen_sort)>len(unique(gen_sort)):
                            print('A1 CAUTION')   
        
                        hardfit=hard_fitness(idship,gen_sort,maxdays,mustvisit, df_port, df_ship, df_portdistance)
                        iter=0
                        nport = len(gen_sort)
                        np = min((nport-3),(len(options)-1))
                        while hardfit==False and iter<20:
                            shuffle(options)
                            shuffle(mustvisit)
                            [options.remove(x) for x in gen_sort if x in options]
                            rrr=options[0:np]
                            gen_sort[2:(nport-1)]=rrr
                            gen_sort = fix_mustvisit(mustvisit,gen_sort,ori,des)
                    
                            if len(gen_sort)>len(unique(gen_sort)):
                                print('A2 CAUTION')
                                
                            if len(gen_sort)>2:
                                arranged_port = copy.deepcopy(gen_sort)
                                if des in arranged_port:
                                    arranged_port.remove(des)
                                if len(unique(arranged_port))>1:
                                    gen_sort = copy.deepcopy(port_arranger_by_distance(list(unique(arranged_port)), df_portdistance))
                                    gen_sort.append(des)
                                    if '' in gen_sort:
                                        gen_sort.remove('')
                            
                            if len(gen_sort)>len(unique(gen_sort)):
                                print('A3 CAUTION')  

                            hardfit=hard_fitness(row.id,gen_sort,maxdays,mustvisit,df_port, df_ship, df_portdistance)
                            iter=iter+1
                        if hardfit==False:
                            iter2=0
                            while (hardfit==False) and (len(gen_sort)>minp) and (iter2<20):
                                gen_sort.remove(gen_sort[-2])
                                gen_sort = fix_mustvisit(mustvisit,gen_sort,ori,des)
                                
                                if len(gen_sort)>len(unique(gen_sort)):
                                    print('A4 CAUTION')

                                if len(gen_sort)>2:
                                    arranged_port = copy.deepcopy(gen_sort)
                                    if des in arranged_port:
                                        arranged_port.remove(des)
                                    if len(unique(arranged_port))>1:
                                        gen_sort = copy.deepcopy(port_arranger_by_distance(list(unique(arranged_port)), df_portdistance))
                                        gen_sort.append(des)
                                        if '' in gen_sort:
                                            gen_sort.remove('')

                                if len(gen_sort)>len(unique(gen_sort)):
                                    print('A5 CAUTION')

                                hardfit=hard_fitness(row.id,gen_sort,maxdays,mustvisit,df_port, df_ship, df_portdistance)
                                iter2=+1
        if '' in gen_sort:
            gen_sort.remove('') 
            if '' in gen_sort: 
                gen_sort.remove('')
        if len(gen_sort)>len(unique(gen_sort)):
            print('CAUTION!')
        genome[j] = gen_sort
        
    return genome

# %%
def mutation_old(genome, ship_input, df_port, df_ship, df_portdistance, num=1,probability=0.5):
    for j in range(len(genome)):
        
        row = ship_input.loc[j]
        listport=eligible_port(ship_input.id[j], df_port, df_ship)
        mv = ship_input.mustVisitPort[j]
        mustvisit = port_mustvisit(mv,df_port)
        gen = copy.deepcopy(genome[j])
        
        gen_sort = copy.deepcopy(gen)
        idship = row.id
        
        ori = row.originPort
        des = row.destinationPort
        mv = row.mustVisitPort
        mustvisit = port_mustvisit(mv,df_port)
        mustminport = len(unique([ori,des,*mustvisit]))

        if row.roundTrip==1:
            minp = max(math.ceil(int(row.minPort)/2),mustminport)
            if minp==2:
                minp=3
            maxdays = max(int(row.minComDays),math.ceil(int(row.maxComDays)/2))
            # maxp = max(minp,math.floor(int(row.maxPort)/2))
        else:
            minp = max(int(row.minPort),mustminport)
            maxdays = row.maxComDays
            # maxp = max(minp,int(row.maxPort))
        
        ori = ship_input.originPort[j]
        des = ship_input.destinationPort[j]
        if(len(gen)>2):
            for _ in range(num):
                if random() > probability:
                    options = copy.deepcopy(listport)
                    [options.remove(x) for x in gen if x in options]
                    if len(options)>0:
                        indexg = randrange(1,(len(gen)-1))
                        indexo = randrange(0,len(options))
                        gen[indexg] = options[indexo]
                        gen = copy.deepcopy(fix_mustvisit(mustvisit,gen,ori,des))
                        gen_sort = copy.deepcopy(gen)
                        if len(gen_sort)>2:
                            arranged_port = copy.deepcopy(gen_sort)
                            # if ori in arranged_port:
                            #     arranged_port.remove(ori)
                            if des in arranged_port:
                                arranged_port.remove(des)
                            if len(unique(arranged_port))>1:
                                gen_sort = copy.deepcopy(port_arranger_by_distance(list(unique(arranged_port)), df_portdistance))
                                # gen_sort.insert(0, ori)
                                gen_sort.append(des)
                        if '' in gen_sort:
                            gen_sort.remove('')
                            if '' in gen_sort: 
                                gen_sort.remove('')       
        
                        # gen_sort = copy.deepcopy(port_arranger_by_distance(gen, df_portdistance))
                        hardfit=hard_fitness(idship,gen_sort,maxdays,mustvisit, df_port, df_ship, df_portdistance)
                        iter=0
                        nport = len(gen_sort)
                        while hardfit==False and iter<20:
                            shuffle(listport)
                            np = min((nport-3),(len(listport)-1))
                            rrr=listport[0:np]
                            gen_sort[2:-1]=rrr
                            gen_sort = fix_mustvisit(mustvisit,gen_sort,ori,des)
                            hardfit=hard_fitness(idship,gen_sort,maxdays,mustvisit,df_port, df_ship, df_portdistance)
                            [listport.remove(r) for r in rrr]
                            iter+=1     
                        if (hardfit==False) & (len(gen_sort)>minp):#iter==20 and hardfit==False:
                            gen_sort.remove(gen_sort[-2])
                            hardfit=hard_fitness(idship,gen_sort,maxdays,mustvisit,df_port, df_ship, df_portdistance)
                            if hardfit==False:
                                gen_sort.remove(gen_sort[-2])
                                hardfit=hard_fitness(idship,gen_sort,maxdays,mustvisit,df_port, df_ship, df_portdistance)
        if '' in gen_sort:
            gen_sort.remove('') 
            if '' in gen_sort: 
                gen_sort.remove('')
        genome[j] = gen_sort
            
    return genome

# %%
def generate_route_beta_1(ship_input, df_port, df_ship, df_portdistance, df_adjustment,
                            df_basefare, df_cargoflat, df_lowpeak, df_pricecargoconfig, 
                            df_priceconfig, df_rev, df_rulecost, season='LOW', npop=15, fitness_limit=2.7,
                            generation_limit=50): #input jumlah iterasi
       
    for i in range(len(ship_input)):
        ship_input.loc[i,'mustVisitPort'] = ship_input.loc[i,'mustVisitPort']+', '+ship_input.loc[i,'homeBase'] 
    
    ## GENERATE INITIAL POPULATION
    # season='LOW'
    # npop=15
    start = time.time()
    Population = []
    if len(ship_input)<3:
        pop_awal = npop*3
    else:
        pop_awal = npop*2

    for i in range(pop_awal):
        Genome=generate_genome(ship_input,df_port,df_ship,df_portdistance)
        Population.append(Genome)

    end = time.time()        
    print(f"time spent for population-generation: {end-start}s")
    
    start = time.time()    
    # fitness_limit = 2.4
    # generation_limit = 20
    for i in range(generation_limit):
        ## CALCULATE FITNESS SCORES
        #fitness ship
        starttime = time.time()
        norm_mean, norm_profit = calculate_fitness(ship_input, Population, df_port, df_portdistance, df_ship, df_rev, df_lowpeak, df_rulecost, 
                                    df_cargoflat, df_pricecargoconfig, df_basefare, df_priceconfig, df_adjustment, season)
        
        #fitness average
        aa=[list(x) for x in zip(*norm_mean)]
        fitscore = [sum(aa[x])/len(aa[x]) for x in range(0,len(aa))]
        fitsort = unique(fitscore)
        top3 = sorted(fitsort)[-3:]
        #If fitness score is high enough
        if (sum(top3)>=fitness_limit) | (len(fitsort)<(npop/3)):
            break
        
        ### NEXT GENERATION SELECTION - Elitism
        # t_pop = [fitscore.index(t) for t in top3]
        # next_generation = [copy.deepcopy(Population[i]) for i in t_pop]
        next_generation = selection_topship(ship_input, Population, norm_mean, 3)
        endtime = time.time()
        print(f"selesai fitness ke-{i} dalam {endtime-starttime} detik")

        ## CROSS & MUTATION
        for j in range(int(npop / 2) - 1):
            parents = copy.deepcopy(selection_random(Population,fitscore))
            offspring_a, offspring_b = copy.deepcopy(single_point_crossover(parents[0], parents[1]))
            offspring_a = copy.deepcopy(mutation(offspring_a, ship_input, df_port, df_ship, df_portdistance))
            offspring_b = copy.deepcopy(mutation(offspring_b, ship_input, df_port, df_ship, df_portdistance))
            next_generation += [offspring_a, offspring_b]

        Population = copy.deepcopy(next_generation)
        
        endtime2 = time.time()
        print(f"selesai mutation ke-{i} dalam {endtime2-endtime} detik")
        

    end = time.time()        
    print(f"time spent for running offspring: {end-start}s")

    if (sum(top3)<fitness_limit) & (len(fitsort)>(npop/3)):
        norm_mean, norm_profit = calculate_fitness(ship_input, Population, df_port, df_portdistance, df_ship, df_rev, df_lowpeak, df_rulecost, 
                                    df_cargoflat, df_pricecargoconfig, df_basefare, df_priceconfig, df_adjustment, season)
    chosen_gen = selection_topship(ship_input, Population, norm_profit, 3)
    #     #fitness average
    #     aa=[list(x) for x in zip(*norm_mean)]
    #     fitscore = [sum(aa[x])/len(aa[x]) for x in range(0,len(aa))]
    #     #choose 3 highest
    #     fitsort = unique(fitscore)
    #     top3 = sorted(fitsort)[-3:]
    # index_top3 = [fitscore.index(t) for t in top3]
    # index_top3 = list(reversed(index_top3))
    # chosen_gen = [copy.deepcopy(Population[p]) for p in index_top3]

    dfs = output_dataframes(ship_input,chosen_gen,df_port, df_ship, df_portdistance, df_adjustment,
                            df_basefare, df_cargoflat, df_lowpeak, df_pricecargoconfig, 
                            df_priceconfig, df_rev, df_rulecost, season)
    header_all = dfs[0]
    est_route_detail = dfs[1]
    cost_all = dfs[2]
    matrix_detail = dfs[3]

    return header_all, est_route_detail, cost_all, matrix_detail

# %%
def calculate_sailingtime(id_ship, id_port1, id_port2, df_ship, df_portdistance):
    #global df_ship, df_port, df_portdistance
    listport = list([id_port1,id_port2])
    df_distance = calculate_totaldistance(listport, df_portdistance)
    df_ship1 = df_ship[df_ship['id'] == id_ship]
    
    df_distance1 = df_distance.fillna(0)
    sailing_time = (df_distance1.iloc[0]['total_nautical']/df_ship1.iloc[0]['speed'])
    
    return sailing_time

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
        if row.coverage>1:
            ruas = row.ruas
            before = (output.ruas<ruas)&(output.type==row.type)
            rowb = output[before]
            if len(rowb)>0:
                rowb = rowb[rowb.ruas==max(rowb.ruas)]
                rowb = rowb.iloc[0]
                new_in = row['in']
                new_out = min(row.out,rowb.onboard)
                            
                new_onboard = min((rowb['onboard']-new_out+row['in']),capacity[row.type])
                new_in = min((new_onboard-rowb.onboard+new_out), capacity[row.type])
            else:
                new_onboard = capacity[row.type]
                new_in = capacity[row.type]
                new_out = row.out
        
            this = (output.ruas==row.ruas)&(output.type==row.type)&(output.port==row.port)
            output.loc[this,'in']=new_in
            output.loc[this,'out']=new_out
            output.loc[this,'onboard']=new_onboard
            output.loc[this,'coverage']=new_onboard/capacity[row.type]

    return output      

# %%
def portlist_corrections_by_constraint(id_ship, portlist, constraint,df_ship,df_port,df_portdistance):
    portlist_size = len(portlist)
    minport = int(constraint[0])
    maxport = int(constraint[1])
    mincomdays = int(constraint[2])
    maxcomdays = int(constraint[3])

    comdays = commission_days(id_ship, portlist,df_ship,df_port,df_portdistance)

    eligible_ports = eligible_port(id_ship, df_port, df_ship)

    # Check if Minnimal Port Fullfilled
    if ((comdays >= mincomdays) & (comdays <= maxcomdays) & 
        (portlist_size >= minport) & (portlist_size <= maxport)):
        portlist_corrected = portlist
    else:
        current_portlist = copy.deepcopy(portlist)
        available_portlist = [port for port in eligible_ports if port not in current_portlist]
        for i in range(maxport - portlist_size):
            size = len(current_portlist)
            if size < minport:
                current_portlist.append(available_portlist[i])
                # print('Adding Port')
            elif size > maxport:
                print('Too Much Port')
                break
            elif (size >= minport) & (size <= maxport):
                # print('Port Suitable')
                minport_fullfilled = True
                break
        
        if minport_fullfilled == True:
            commdays_from_current_portlist = commission_days(id_ship, current_portlist,df_ship,df_port,df_portdistance)
            if commdays_from_current_portlist < mincomdays:
                loop_commdays = copy.copy(commdays_from_current_portlist)
                available_portlist = [port for port in eligible_ports if port not in current_portlist]
                iterator = 0
                while loop_commdays < mincomdays:
                    current_portlist.append(available_portlist[iterator])
                    loop_commdays = commission_days(id_ship, current_portlist,df_ship,df_port,df_portdistance)
                    iterator = iterator + 1
                portlist_corrected = current_portlist
            elif commdays_from_current_portlist > maxcomdays:
                print('Out of Max Commission Days')
            elif ((commdays_from_current_portlist >= mincomdays) & (commdays_from_current_portlist <= maxcomdays)):
                portlist_corrected = current_portlist
                
    return portlist_corrected

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


