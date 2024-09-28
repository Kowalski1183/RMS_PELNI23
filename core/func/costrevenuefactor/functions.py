from core.func.general_function import *
from core.schemas.schemas import *
import pandas as pd


def cal_cost_revenue_factor(request_data: RouteCostRequest):

    df_ship = retrieve_ship('001')
    df_port = retrieve_port('001')
    df_rulecost = retrieve_rulecost()
    df_cargoflat = retrieve_cargoflat()
    df_basefare = retrieve_basefare()
    df_priceconfig = retrieve_priceconfig()
    df_adjustment = retrieve_adjustment()
    df_portdistance = retrieve_portdistance()
    df_revenue = retrieve_revenue(2)
    df_lowpeak = retrieve_lowpeak()
    df_pricecargoconfig = retrieve_pricecargoconfig()

    dfRevLow, dfRevPeak = separate_rev(
        df_revenue=df_revenue, df_lowpeak=df_lowpeak)

    if request_data.season == 'REGULAR':
        seasons = 'LOW'
        dfRev = dfRevLow
    else:
        seasons = request_data.season
        dfRev = dfRevPeak

    prediction = calculate_prediction(request_data.idPort,
                                      df_rev=dfRev)

    cd_all = covered_demand(prediction, seasons)

    # distance = calculate_distanceperport(request_data.idPort,df_portdistance)
    factor, movement = cal_factor(request_data.idShip,
                                  request_data.idPort, cd_all, df_ship)

    movement['total'] = movement['total'].astype(int)
    npax = movement.loc[movement['type'] == 'PASSENGER', 'total'].sum()
    
    revenue, matrix = cal_revenue(request_data.idPort,
                                  None,
                                  movement,
                                  df_cargoflat,
                                  df_pricecargoconfig,
                                  df_lowpeak,
                                  df_basefare,
                                  df_priceconfig,
                                  df_adjustment,
                                  df_portdistance,
                                  season=seasons)

    merged1 = pd.merge(revenue, factor, how='right', left_on=[
                       'origin', 'type', 'ruas'], right_on=['port', 'type', 'ruas'])
    maxruas = copy.deepcopy(max(merged1['ruas']))
    merged1 = merged1.drop(list(merged1.loc[merged1['ruas'] == maxruas].index))
    merged2 = pd.merge(factor, revenue, how='outer', left_on=[
                       'port', 'ruas', 'type'], right_on=['origin', 'ruas', 'type']).drop(columns=['origin'])
    merged1 = merged1.fillna(0)
    merged2 = merged2.fillna(0)
    

    if len(merged2) > 0:
        # typelist = list(revenue['type'].unique())
        # maxruas = copy.deepcopy(max(merged2['ruas']))
        # merged2 = merged2[(merged2['ruas'] == maxruas) &
        #                   (merged2['type'].isin(typelist))]
        # merged_df = pd.concat([merged1, merged2], ignore_index=True)
        # merged_df = merged_df.drop(columns=['origin'])
        # merged_df = merged_df.fillna(0)
        
        typelist = list(revenue['type'].unique())
        if 'PASSENGER' not in typelist:
            typelist = typelist.append('PASSENGER')
        maxruas = copy.deepcopy(max(merged2['ruas']))
        merged2 = merged2[(merged2['ruas']==maxruas)&(merged2['type'].isin(typelist))]
        df_join = pd.concat([merged1,merged2], ignore_index=True)
        merged_df = df_join[(df_join['type'].isin(typelist))]
        
    else:
        merged_df = merged1.drop(columns=['origin'])

    merged_df.rename(columns={
        'port': 'portOrigin',
        'coverage': 'factor',
        'total': 'estTotal',
        'in': 'estUp',
        'out': 'estDown',
        'revenue': 'estRevenue',
        'onboard': 'estOnboard',
        'distance': 'portDistance'
    }, inplace=True)

    # Convert 'int64' columns to regular integers
    merged_df['ruas'] = merged_df['ruas'].astype('int8')
    merged_df['estTotal'] = merged_df['estTotal'].astype(int)
    merged_df['estUp'] = merged_df['estUp'].astype(int)
    merged_df['estDown'] = merged_df['estDown'].astype(int)
    merged_df['estOnboard'] = merged_df['estOnboard'].astype(int)

    # Reorder columns
    merged_list = merged_df[['portOrigin', 'ruas', 'type', 'portDistance',
                             'estTotal', 'estRevenue', 'estUp', 'estDown', 'estOnboard', 'factor']]

    merged_df['factor'] = round(merged_df['factor'], 2)
    merged_df['estOnboard'] = round(merged_df['estOnboard'], 2)

    # Convert the DataFrame to a list of dictionaries
    merged_list_dict = merged_list.to_dict(orient='records')

    matrix['ruas_origin'] = matrix['ruas_origin'].astype('int8')
    matrix['ruas_destination'] = matrix['ruas_destination'].astype('int8')
    matrix.rename(columns={
        'ruas_origin': 'ruasOrigin',
        'ruas_destination': 'ruasDestination'
    }, inplace=True)
    matrix_dict = matrix.to_dict(orient='records')

    # Calculate the total and revenue
    total_revenue_dict = {
        'factor': merged_df['factor'].sum(),
        'onboard': int(merged_df['estOnboard'].sum()),
        'distance': merged_df['portDistance'].sum(),
        "total": int(merged_df['estTotal'].sum()),
        "revenue": merged_df['estRevenue'].sum(),
    }

    revenue = {
        "revenueDetail": merged_list_dict,
        "totalRevenue": total_revenue_dict,
        "estMatrixTotal": matrix_dict
    }

    costdetail, total = calculate_routecost(id_ship=request_data.idShip,
                                            id_port=request_data.idPort,
                                            df_rulecost=df_rulecost,
                                            npax=npax,
                                            df_ship=df_ship,
                                            df_port=df_port,
                                            df_portdistance=df_portdistance)

    columnsint = ['expenseperday', 'sailingtime', 'berthingtime', 'npax']
    for column in columnsint:
        costdetail[column].fillna(0, inplace=True)
    columnsstr = ['idship', 'idport', 'time']
    for column in columnsstr:
        costdetail[column].fillna('NULL', inplace=True)

    timecal = pd.DataFrame(calculate_time(
        request_data.idShip, request_data.idPort, df_ship, df_port, df_portdistance))
    timecal.set_index('Name', inplace=True)
    sailing_time = timecal.loc['Sailing Time']['Amount']
    berthing_time = timecal.loc['Berthing Time']['Amount']
    comm = timecal.loc['Commission Days']['Amount']

    costdetail_list = costdetail
    costdetail_list.rename(columns={
        'idrulecost': 'idRuleCost',
        'npax': 'npax',
        'sailingtime': 'sailingTime',
        'berthingtime': 'berthingTime',
        'cost': 'subtotalCost'
    }, inplace=True)

    costdetail_list['npax'] = costdetail_list['npax'].astype(int)
    costdetail_list['sailingTime'] = costdetail_list['sailingTime'].astype(int)
    costdetail_list['berthingTime'] = costdetail_list['berthingTime'].astype(
        int)
    costdetail_list['expenseperday'] = costdetail_list['expenseperday'].astype(
        int)

    costdetail_json = costdetail_list.to_dict(orient='records')

    cost_route = {
        'data': costdetail_json,
        'totSail': float(round(sailing_time, 2)),
        'totBerth': float(round(berthing_time, 2)),
        'comDays': int(comm),
        'totalCost': float(round(total['totalcost'], 2))
    }

    return cost_route, revenue
