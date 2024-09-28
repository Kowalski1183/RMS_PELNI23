# %% [markdown]
# Calculator Functions for Optimizer

# %%
import pandas as pd
# import import_ipynb
import datetime
from .data_preparation import *
import math
import itertools

# %%
# df_port = pd.DataFrame(retrieve_port('001'))
# df_ship = pd.DataFrame(retrieve_ship('001'))
# df_portdistance = pd.DataFrame(retrieve_portdistance())
# df_revenue = pd.DataFrame(retrieve_revenue(type_rev))
# df_maintenance = pd.DataFrame(retrieve_maintenance())
# df_tide = pd.DataFrame(retrieve_tide())
# df_priceconfig = pd.DataFrame(retrieve_priceconfig())
# df_basefare = pd.DataFrame(retrieve_basefare())
# df_rulecost = pd.DataFrame(retrieve_rulecost())
# df_lowpeak = pd.DataFrame(retrieve_lowpeak())
# df_pricecargoconfig = pd.DataFrame(retrieve_pricecargoconfig())
# df_adjustment = pd.DataFrame(retrieve_adjustment())

# %%
def calculate_paxprice(distance, dot):
    df_priceconfig = pd.DataFrame(retrieve_priceconfig())
    df_priceconfig = df_priceconfig[(df_priceconfig['mindistance'] <= distance) & (df_priceconfig['maxdistance'] >= distance)]

    dot_date = datetime.datetime.strptime(dot, '%Y-%m-%d').date()
    df_lowpeak = retrieve_lowpeak()
    df_lowpeak = df_lowpeak[(df_lowpeak['startdate'] <= dot_date) & (df_lowpeak['enddate'] >= dot_date)]

    df_basefare = retrieve_basefare()
    dot_datetime = datetime.datetime.strptime(dot, '%Y-%m-%d')
    df_basefare = df_basefare[(df_basefare['startdate'] <= dot_datetime) & (df_basefare['enddate'] >= dot_datetime)]
    df_basefare = df_basefare[df_basefare['typefare'] == 'PAX']
    df_basefare = df_basefare[df_basefare['type'] == df_lowpeak.iloc[0]['type']]

    df_adjustment = pd.DataFrame(retrieve_adjustment())
    adjustmentconfig = df_adjustment['value'].values[0]

    if df_priceconfig.iloc[0]['mindistance'] == 0:
        koefjarak = df_priceconfig['distancecoeff']
    elif df_priceconfig.iloc[0]['mindistance'] !=0:
        koefjarak = df_priceconfig.iloc[0]['distancecoeff'] + ((distance - df_priceconfig.iloc[0]['mindistance'] - 1) * 
                                                               df_priceconfig.iloc[0]['coeff'])

    rp = round(koefjarak * df_basefare.iloc[0]['basefare'], -3)

    if distance < 1000:
        pnp = rp
    elif distance >= 1000:
        pnp = 0.9 * rp

    pnpmile = round(pnp/distance, 2)
    tarif = round(distance * (df_priceconfig.iloc[0]['coeff']) * (df_priceconfig.iloc[0]['pangsa']) * pnpmile, -3)
    adjust = (100 - adjustmentconfig)/100
    tarifakhir = round(adjust * tarif, -3)

    data = {'Name' : ['Tarif', 'Tarif Akhir'],
            'Amount' : [tarif, tarifakhir]}
    df = pd.DataFrame(data)
    return df

# %%
# calculate_paxprice(distance=, dot=)
# calculate_paxprice(1000, '2023-04-12')

# %%
def calculate_cargoprice(distance, dot):
    df_pricecargoconfig = retrieve_pricecargoconfig()
    df_pricecargoconfig = df_pricecargoconfig[(df_pricecargoconfig['mindistance'] <= distance) & 
                                              (df_pricecargoconfig['maxdistance'] >= distance)]

    dot_date = datetime.datetime.strptime(dot, '%Y-%m-%d').date()
    df_lowpeak = retrieve_lowpeak()
    df_lowpeak = df_lowpeak[(df_lowpeak['startdate'] <= dot_date) & (df_lowpeak['enddate'] >= dot_date)]

    df_basefare = pd.DataFrame(retrieve_basefare())
    dot_datetime = datetime.datetime.strptime(dot, '%Y-%m-%d')
    df_basefare = df_basefare[(df_basefare['startdate'] <= dot_datetime) & (df_basefare['enddate'] >= dot_datetime)]
    df_basefare = df_basefare[df_basefare['typefare'] == 'CARGO']
    df_basefare = df_basefare[df_basefare['type'] == df_lowpeak.iloc[0]['type']]

    if df_pricecargoconfig.iloc[0]['mindistance'] == 0:
        koefjarak = df_pricecargoconfig['distancecoeff']
    elif df_pricecargoconfig.iloc[0]['mindistance'] != 0:
        koefjarak = df_pricecargoconfig.iloc[0]['distancecoeff'] + ((distance - df_pricecargoconfig.iloc[0]['mindistance'] - 1) 
                                                                    * df_pricecargoconfig.iloc[0]['coeff'])

    tarif = koefjarak * df_basefare.iloc[0]['basefare']
    
    data = {'Name' : ['Tarif'],
            'Amount' : [tarif]}
    df = pd.DataFrame(data)
    return df

# %%
# calculate_cargoprice(distance=, dot=)
# calculate_cargoprice(200, '2023-04-12')

# %%
def calculate_totaldistance(id_port):
    df_portdistance = retrieve_portdistance()
    size = len(id_port) - 1
    origin = []
    destination = []

    for i in range(size):
        a = id_port[i]
        b = id_port[(i+1)]
        origin.append(a)
        destination.append(b)
           
    od_pair = pd.DataFrame({'id_origin' : origin, 'id_destination' : destination})

    df_join = pd.merge(df_portdistance, od_pair, how='inner', left_on=('id_origin', 'id_destination'), 
                       right_on=('id_origin', 'id_destination'))

    total_nautical = df_join['nautical'].sum()
    total_commercial = df_join['commercial'].sum()
    data = {'total_nautical' : total_nautical,
            'total_commercial' : total_commercial}
    df = pd.DataFrame(data, index=[0])
    return df

# %%
# calculate_totaldistance(id_port=[])
# calculate_totaldistance(['19887a24-9011-425f-bc0c-5f5cd512ba5a', '6a01e7c5-05e6-4ab7-a706-f0936f32a837', 'fa422e2a-605a-4ca7-954b-8e2b08db4bff', 'c2f757c2-442b-4fce-9a53-4292baf0f776'])

# %%
def calculate_distanceperport(id_port):     
    od_pair = pd.DataFrame((itertools.permutations(id_port, 2)), columns=['id_origin', 'id_destination'])
    
    df_cargoflat = pd.DataFrame(retrieve_portdistance())
    df_join = pd.merge(df_cargoflat, od_pair, how='inner', left_on=('id_origin', 'id_destination'), 
                       right_on=('id_origin', 'id_destination')).drop(columns = ['commercial'])
    return df_join

# %%
# calculate_distanceperport(id_port=[])
# calculate_distanceperport(['19887a24-9011-425f-bc0c-5f5cd512ba5a', '6a01e7c5-05e6-4ab7-a706-f0936f32a837', 'fa422e2a-605a-4ca7-954b-8e2b08db4bff', 'c2f757c2-442b-4fce-9a53-4292baf0f776'])
# calculate_distanceperport(id_port = ['3f3e554e-0f50-43d2-bcd1-0a9d08ba8a25', 'c672024c-821d-4c32-b162-612017b50b4c', 'be786d38-6e83-4805-b8a7-60dde4fd2699'])

# %%
def calculate_prediction(id_port, season):
    df_revenue = pd.DataFrame(retrieve_revenue(2)).reset_index(drop=True)
    df_revenue['depdate'] = pd.to_datetime(df_revenue['depdate'])
    
    df_lowpeak = pd.DataFrame(retrieve_lowpeak())
    df_lowpeak['startdate'] = pd.to_datetime(df_lowpeak['startdate'])
    df_lowpeak['enddate'] = pd.to_datetime(df_lowpeak['enddate'])
    peak_season = df_lowpeak[df_lowpeak['type'] == 'PEAK'].reset_index(drop=True)
    low_season = df_lowpeak[df_lowpeak['type'] == 'LOW'].reset_index(drop=True)

    df_peakrev = pd.DataFrame()
    for idx in peak_season.index:
        peak_revenue = df_revenue[(df_revenue['depdate'] >= peak_season['startdate'][idx]) & 
                                  (df_revenue['depdate'] <= peak_season['enddate'][idx])]
        df_peakrev = pd.concat([df_peakrev, peak_revenue])

    df_peakrev = df_peakrev.reset_index(drop=True)

    df_lowrev = pd.concat([df_revenue, df_peakrev]).drop_duplicates(keep=False).reset_index(drop=True)

    if season == 'PEAK':
        df_rev = df_peakrev
    elif season == 'LOW':
        df_rev = df_lowrev

    od_pair = pd.DataFrame((itertools.permutations(id_port, 2)), columns=['origin', 'destination'])
    
    df_join = pd.merge(df_rev, od_pair, how='inner', left_on=('origin', 'destination'), right_on=('origin', 'destination'))

    df = df_join.groupby(['origin', 'destination', 'type'], as_index=False)[['revenue', 'total']].describe()
    return df

# %%
# calculate_prediction(id_port=[])
# calculate_prediction(['3f3e554e-0f50-43d2-bcd1-0a9d08ba8a25', 'c672024c-821d-4c32-b162-612017b50b4c', 'be786d38-6e83-4805-b8a7-60dde4fd2699'], 'LOW')

# %%
def calculate_time(id_ship, id_port):
    df_distance = calculate_totaldistance(id_port)
    df_ship = pd.DataFrame(retrieve_ship('001'))
    df_ship = df_ship[df_ship['id'] == id_ship]
    sailing_time = (df_distance.iloc[0]['total_nautical']/df_ship.iloc[0]['speed'])/24
    
    df_port = pd.DataFrame(retrieve_port('001'))
    df_port = df_port[df_port['id'].isin(id_port)]
    berthing_time = df_port['avgberth'].sum()/24

    comm_days = math.ceil(berthing_time + sailing_time)
    
    data = {'Name' : ['Sailing Time', 'Berthing Time', 'Commission Days'],
            'Amount' : [sailing_time, berthing_time, comm_days]}
    df = pd.DataFrame(data)
    return df

# %%
# calculate_time(id_ship=, id_port=[])
# calculate_time('2c911422-b6d9-47c9-93d5-71c8d339d3e9', ['b166ff1e-a3f6-4899-a4f4-27f7ad076b6a', '09d8f97b-a99e-4644-9460-f8b7eb7f5afb', '05aa9d6c-6999-4f7a-b4d6-7dddca9db337'])

# %%
def calculate_routecost(id_ship, id_port, season, time=False):
    df_rulecost = pd.DataFrame(retrieve_rulecost())
    df_rulecost = df_rulecost[df_rulecost['localtime'].isnull()]
    df_rulecost['ispax'] = df_rulecost['ispax'].fillna(False)
    df_rulecost['issailing'] = df_rulecost['issailing'].fillna(False)
    df_rulecost['isberthing'] = df_rulecost['isberthing'].fillna(False)

    df_ship = pd.DataFrame(retrieve_ship('001'))
    df_ship = df_ship[df_ship['id'] == id_ship]
    df_ship = df_ship.fillna(0)

    totalcost = 0

    if time != False:
        print('Parameter Time is not False')

    costdetail = pd.DataFrame(columns=['idrulecost','name', 'npax', 'sailingtime', 'berthingtime', 'idship', 'idport', 'time', 
                                       'expenseperday', 'cost'])

    for id in df_rulecost.index:
        if (df_rulecost['ispax'][id] == False) & (df_rulecost['issailing'][id] == False) & (df_rulecost['isberthing'][id] == False):
            if (df_rulecost['idship'][id] == None) & (df_rulecost['idport'][id] == None):
                name = df_rulecost['name']
                cost = df_rulecost['cost']
                idrulecost = df_rulecost['idrulecost']

                add_cost = pd.DataFrame({'idrulecost':idrulecost,'name' : name, 'cost' : cost}, index=[id])
                costdetail = pd.concat([costdetail, add_cost])
            else: 
                if (df_rulecost['idship'][id] == None):
                    if (df_rulecost['idport'][id] != None):
                        for i in id_port:
                            if (df_rulecost['idport'][id] == i):
                                name = df_rulecost['name']
                                cost = df_rulecost['cost']
                                idrulecost = df_rulecost['idrulecost']
                                ship_rule = df_rulecost['idship'][id]
                                port_rule = df_rulecost['idport'][id]

                                add_cost = pd.DataFrame({'idrulecost': idrulecost,'name' : name, 'cost' : cost, 'idship' : ship_rule, 
                                                         'idport' : port_rule}, index=[id])
                                costdetail = pd.concat([costdetail, add_cost])
                else:
                    if (df_rulecost['idship'][id] == id_ship):                 
                        name = df_rulecost['name']
                        cost = df_rulecost['cost']
                        idrulecost = df_rulecost['idrulecost']
                        ship_rule = df_rulecost['idship'][id]

                        add_cost = pd.DataFrame({'idrulecost':idrulecost,'name' : name, 'cost' : cost, 'idship' : id_ship}, index=[id])
                        costdetail = pd.concat([costdetail, add_cost])
                
        else:
            timecal = pd.DataFrame(calculate_time(id_ship, id_port))
            timecal.set_index('Name', inplace=True)
            sailing_time = timecal.loc['Sailing Time']['Amount']
            berthing_time = timecal.loc['Berthing Time']['Amount']

            if df_rulecost['isberthing'][id] == True:
                if berthing_time != 0:
                    cost = berthing_time * df_rulecost['perday'] * df_rulecost['cost']
                    
                    name = df_rulecost['name']
                    expenseperday = df_rulecost['perday']
                    ship_rule = df_rulecost['idship'][id]
                    idrulecost = df_rulecost['idrulecost']

                    add_cost = pd.DataFrame({'idrulecost':idrulecost,'name' : name, 'idship' : ship_rule, 'berthingtime' : berthing_time, 
                                             'expenseperday' : expenseperday, 'cost' : cost}, index=[id])
                    costdetail = pd.concat([costdetail, add_cost])
                else:
                    continue

            elif df_rulecost['issailing'][id] == True:
                if sailing_time != 0:
                    cost = sailing_time * df_rulecost['perday'] * df_rulecost['cost']

                    name = df_rulecost['name']
                    expenseperday = df_rulecost['perday']
                    ship_rule = df_rulecost['idship'][id]
                    idrulecost = df_rulecost['idrulecost']

                    add_cost = pd.DataFrame({'idrulecost':idrulecost,'name' : name, 'idship' : ship_rule, 'sailingtime' : sailing_time, 
                                             'expenseperday' : expenseperday, 'cost' : cost}, index=[id])
                    costdetail = pd.concat([costdetail, add_cost])
                else:
                    continue

            elif df_rulecost['ispax'][id] == True:
                if len(id_port) <= 1:
                    continue
                else:
                    prediction = pd.DataFrame(calculate_prediction(id_port, season))
                    pax = prediction[prediction['type'] == 'PASSENGER']
                    npax = prediction['total', '50%'].sum()
                    cost = npax * df_rulecost['cost']

                    name = df_rulecost['name']
                    ship_rule = df_rulecost['idship'][id]
                    idrulecost = df_rulecost['idrulecost']

                    add_cost = pd.DataFrame({'idrulecost':idrulecost,'name' : name, 'idship' : ship_rule, 'npax' : npax, 'cost' : cost}, index=[id])
                    costdetail = pd.concat([costdetail, add_cost])

    totalcost = costdetail['cost'].sum()
    data = {'totalcost' : totalcost}
    total = pd.DataFrame(data, index=[0])
    # display(costdetail, total)
    
    # Replace NaN values with 0 in 'expenseperday' and 'cost' columns
    columnsint = ['cost','expenseperday','sailingtime','berthingtime']
    for column in columnsint:
        costdetail[column].fillna(0,inplace=True)
    columnsstr = ['name','npax','idship','idport','time']
    for column in columnsstr:
        costdetail[column].fillna('NULL',inplace=True)
    

    # Convert DataFrame rows to list of dictionaries with desired mapping
    costdetail_list = []
    for _, row in costdetail.iterrows():
        detail = {
            'idrulecost' : row['idrulecost'],
            'name': row['name'],
            'npax': row['npax'],
            'sailingtime': round(row['sailingtime'],2),
            'berthingtime': round(row['berthingtime'],2),
            'idship': row['idship'],
            'idport': row['idport'],
            'time': row['time'],
            'expenseperday': round(row['expenseperday'],2),
            'cost': round(row['cost'],2)
        }
        costdetail_list.append(detail)

    # Calculate the total cost based on the costdetail data
    totcost = costdetail['cost'].sum()

    # Create the final JSON structure with the list of costdetail data
    json_data = {
        'data': costdetail_list,
        'totalcost': round(totcost, 2)
    }
    return json_data

# %%
# calculate_routecost(id_ship=,id_port=[], time=False, season=)
# calculate_routecost('fe2b4186-22d9-476b-b2ac-d01f5b10ceaa', ['b2674a83-0456-4666-893d-7c7522f9232b'], 'PEAK', False)

# %%
def calculate_revenue(id_port, season, dot):
    cal_distance = pd.DataFrame(calculate_distanceperport(id_port))
    cal_distance = cal_distance.drop(columns=['origin', 'destination'])
    cal_distance = cal_distance.rename(columns={'id_origin' : 'origin', 'id_destination' : 'destination'})
    cal_distance.columns = pd.MultiIndex.from_product([list(cal_distance.columns), ['']])

    cal_prediction = pd.DataFrame(calculate_prediction(id_port, season))

    distance_prediction = pd.merge(cal_prediction, cal_distance, on=(('origin', ''), ('destination', '')), how='outer')
    distance_prediction = distance_prediction[distance_prediction['type'].notna()]

    cargoflat = pd.DataFrame(retrieve_cargoflat())
    cargoflat.loc[cargoflat['type'] == 'DRY', 'type'] = 'DRY CONTAINER'
    cargoflat_m = cargoflat.rename(columns={'id_origin' : 'origin', 'id_destination' : 'destination', 'fare' : 'cargoflat_fare'})
    cargoflat_m.columns = pd.MultiIndex.from_product([list(cargoflat_m.columns), ['']])

    distance_prediction_cargoflat = pd.merge(distance_prediction, cargoflat_m, how='left', 
                                             left_on=(('origin', ''), ('destination', ''), ('type', '')), 
                                             right_on=(('origin', ''), ('destination', ''), ('type', '')))

    calculate_revenue = pd.DataFrame(columns=['origin', 'destination', 'type', 'total', 'revenue'])

    for i in distance_prediction_cargoflat.index:
        distance = distance_prediction_cargoflat.loc[i, ('nautical', '')]

        if distance_prediction_cargoflat.loc[i, ('type', '')] == 'PASSENGER':

            cal_paxprice = pd.DataFrame(calculate_paxprice(distance, dot))
            cal_paxprice.set_index('Name', inplace=True)
            
            origin = distance_prediction_cargoflat.loc[i, ('origin', '')]
            destination = distance_prediction_cargoflat.loc[i, ('destination', '')]
            type = distance_prediction_cargoflat.loc[i, ('type', '')]
            paxprice = cal_paxprice['Amount']['Tarif Akhir']
            total_median = distance_prediction_cargoflat.loc[i, ('total', '50%')]
            
            revenue = paxprice * total_median
            
            add_revenue = pd.DataFrame({'origin' : origin, 'destination' : destination, 'type' : type, 'total' : total_median, 
                                        'revenue' : revenue}, index=[i])
            calculate_revenue = pd.concat([calculate_revenue, add_revenue])
        elif distance_prediction_cargoflat.loc[i, ('type', '')] == 'GENERAL CARGO':
            cal_cargoprice = pd.DataFrame(calculate_cargoprice(distance, dot))
            cal_cargoprice.set_index('Name', inplace=True)
            
            origin = distance_prediction_cargoflat.loc[i, ('origin', '')]
            destination = distance_prediction_cargoflat.loc[i, ('destination', '')]
            type = distance_prediction_cargoflat.loc[i, ('type', '')]
            cargoprice = cal_cargoprice['Amount']['Tarif']
            total_median = cal_prediction.loc[i, ('total', '50%')]
            
            revenue = cargoprice * total_median
            
            add_revenue = pd.DataFrame({'origin' : origin, 'destination' : destination, 'type' : type, 'total' : total_median, 
                                        'revenue' : revenue}, index=[i])
            calculate_revenue = pd.concat([calculate_revenue, add_revenue])
        else:
            if distance_prediction_cargoflat.loc[i, ('type', '')] == 'REDPACK':          
                origin = distance_prediction_cargoflat.loc[i, ('origin', '')]
                destination = distance_prediction_cargoflat.loc[i, ('destination', '')]
                type = distance_prediction_cargoflat.loc[i, ('type', '')]
                cargoprice = distance_prediction_cargoflat.loc[i, ('cargoflat_fare', '')]
                total_median = cal_prediction.loc[i, ('total', '50%')]
                
                revenue = cargoprice * total_median
                
                add_revenue = pd.DataFrame({'origin' : origin, 'destination' : destination, 'type' : type, 'total' : total_median, 
                                            'revenue' : revenue}, index=[i])
                calculate_revenue = pd.concat([calculate_revenue, add_revenue])
            elif distance_prediction_cargoflat.loc[i, ('type', '')] == 'MOTOR':          
                origin = distance_prediction_cargoflat.loc[i, ('origin', '')]
                destination = distance_prediction_cargoflat.loc[i, ('destination', '')]
                type = distance_prediction_cargoflat.loc[i, ('type', '')]
                cargoprice = distance_prediction_cargoflat.loc[i, ('cargoflat_fare', '')]
                total_median = cal_prediction.loc[i, ('total', '50%')]
                
                revenue = cargoprice * total_median
                
                add_revenue = pd.DataFrame({'origin' : origin, 'destination' : destination, 'type' : type, 'total' : total_median, 
                                            'revenue' : revenue}, index=[i])
                calculate_revenue = pd.concat([calculate_revenue, add_revenue])
            elif distance_prediction_cargoflat.loc[i, ('type', '')] == 'MOBIL':          
                origin = distance_prediction_cargoflat.loc[i, ('origin', '')]
                destination = distance_prediction_cargoflat.loc[i, ('destination', '')]
                type = distance_prediction_cargoflat.loc[i, ('type', '')]
                cargoprice = distance_prediction_cargoflat.loc[i, ('cargoflat_fare', '')]
                total_median = cal_prediction.loc[i, ('total', '50%')]
                
                revenue = cargoprice * total_median
                
                add_revenue = pd.DataFrame({'origin' : origin, 'destination' : destination, 'type' : type, 'total' : total_median, 
                                            'revenue' : revenue}, index=[i])
                calculate_revenue = pd.concat([calculate_revenue, add_revenue])
            elif distance_prediction_cargoflat.loc[i, ('type', '')] == 'TRUK':          
                origin = distance_prediction_cargoflat.loc[i, ('origin', '')]
                destination = distance_prediction_cargoflat.loc[i, ('destination', '')]
                type = distance_prediction_cargoflat.loc[i, ('type', '')]
                cargoprice = distance_prediction_cargoflat.loc[i, ('cargoflat_fare', '')]
                total_median = cal_prediction.loc[i, ('total', '50%')]
                
                revenue = cargoprice * total_median
                
                add_revenue = pd.DataFrame({'origin' : origin, 'destination' : destination, 'type' : type, 'total' : total_median, 
                                            'revenue' : revenue}, index=[i])
                calculate_revenue = pd.concat([calculate_revenue, add_revenue])
            elif distance_prediction_cargoflat.loc[i, ('type', '')] == 'DRY CONTAINER':          
                origin = distance_prediction_cargoflat.loc[i, ('origin', '')]
                destination = distance_prediction_cargoflat.loc[i, ('destination', '')]
                type = distance_prediction_cargoflat.loc[i, ('type', '')]
                cargoprice = distance_prediction_cargoflat.loc[i, ('cargoflat_fare', '')]
                total_median = cal_prediction.loc[i, ('total', '50%')]
                
                revenue = cargoprice * total_median
                
                add_revenue = pd.DataFrame({'origin' : origin, 'destination' : destination, 'type' : type, 'total' : total_median, 
                                            'revenue' : revenue}, index=[i])
                calculate_revenue = pd.concat([calculate_revenue, add_revenue])
            elif distance_prediction_cargoflat.loc[i, ('type', '')] == 'CONTAINER':          
                origin = distance_prediction_cargoflat.loc[i, ('origin', '')]
                destination = distance_prediction_cargoflat.loc[i, ('destination', '')]
                type = distance_prediction_cargoflat.loc[i, ('type', '')]
                cargoprice = distance_prediction_cargoflat.loc[i, ('cargoflat_fare', '')]
                total_median = cal_prediction.loc[i, ('total', '50%')]
                
                revenue = cargoprice * total_median
                
                add_revenue = pd.DataFrame({'origin' : origin, 'destination' : destination, 'type' : type, 'total' : total_median, 
                                            'revenue' : revenue}, index=[i])
                calculate_revenue = pd.concat([calculate_revenue, add_revenue])
            elif distance_prediction_cargoflat.loc[i, ('type', '')] == 'REEFER CONTAINER':          
                origin = distance_prediction_cargoflat.loc[i, ('origin', '')]
                destination = distance_prediction_cargoflat.loc[i, ('destination', '')]
                type = distance_prediction_cargoflat.loc[i, ('type', '')]
                cargoprice = distance_prediction_cargoflat.loc[i, ('cargoflat_fare', '')]
                total_median = cal_prediction.loc[i, ('total', '50%')]
                
                revenue = cargoprice * total_median
                
                add_revenue = pd.DataFrame({'origin' : origin, 'destination' : destination, 'type' : type, 'total' : total_median, 
                                            'revenue' : revenue}, index=[i])
                calculate_revenue = pd.concat([calculate_revenue, add_revenue])

    calculate_revenue['revenue'] = calculate_revenue['revenue'].fillna(0)
    grouped_revenue = calculate_revenue.groupby(['type', 'origin'])[['total', 'revenue']].sum()
      
    return grouped_revenue

# %%
# calculate_revenue(id_port=[], season=, dot=)
# calculate_revenue(['d4787f56-56d0-45ae-8822-ce55b720357a', '5267592e-5d29-4fdf-8975-7ade3bd50b39', '25bbc269-e619-47e4-ad47-7c90cb898da8', 'c2f757c2-442b-4fce-9a53-4292baf0f776', '37463b21-4d37-4327-839a-56985cdc8aa0', '123da461-dbcb-4de3-9a2a-3575379a4d14', '265e3742-f63d-47a8-941f-11effec8a68a'], 'PEAK', '2023-01-01')


