from fastapi import APIRouter

from core.schemas.schemas import *
# from core.func.calculator import *
# from core.func.data_preparation_k import *
from core.func.general_function import *

from core.endpoint import COSTOFROUTE

router = APIRouter()

@router.post(COSTOFROUTE,tags=["Calculator Service"])
def cal_cost_of_route(request_data: RouteCostRequest):
    df_rulecost=retrieve_rulecost()
    df_ship=retrieve_ship('001')
    df_port=retrieve_port('001')
    df_portdistance=retrieve_portdistance()
    df_revenue=retrieve_revenue(2)
    df_lowpeak=retrieve_lowpeak()

    # route_list = []
    # for row in request_data.idPort:
    #     route = df_port.loc[df_port['id']==row]
    #     portName = route.iloc[0]['name']
    #     route_list.append(portName)

    dfRevLow, dfRevPeak = separate_rev(df_revenue=df_revenue, df_lowpeak=df_lowpeak)

    if request_data.season=='REGULAR':
        seasons='LOW'
        dfRev = dfRevLow
    else:
        seasons=request_data.season
        dfRev = dfRevPeak

    prediction = calculate_prediction(request_data.idPort,
                                      df_rev=dfRev)
    
    cd_all = covered_demand(prediction, seasons)
    npax = cd_all.loc[cd_all['type']=='PASSENGER', 'total'].sum()
    
    # distance = calculate_distanceperport(request_data.idPort,df_portdistance)

    commission = commission_days(request_data.idShip,request_data.idPort,df_ship
                                 , df_port, df_portdistance)

    data = calculate_routecost(id_ship=request_data.idShip,
                               id_port=request_data.idPort,
                               df_rulecost=df_rulecost, 
                               npax=npax,
                               df_ship=df_ship, 
                               df_port=df_port, 
                               df_portdistance=df_portdistance)
    
    # Replace NaN values with 0 in 'expenseperday' and 'cost' columns
    columnsint = ['cost','expenseperday','sailingtime','berthingtime']
    for column in columnsint:
        data[0][column].fillna(0,inplace=True)
    columnsstr = ['name','npax','idship','idport','time']
    for column in columnsstr:
        data[0][column].fillna('NULL',inplace=True)

    costdetail_list = data[0].to_dict(orient='records')
    matrix_total = cd_all.to_dict(orient='records')
    totcost = data[0]['cost'].sum()

    # Create the final JSON structure with the list of costdetail data
    json_data = {
        'data': costdetail_list,
        'matrixTotal':matrix_total,
        'commissionDays': commission,
        'totalCost': round(totcost, 2)
    }

    return json_data

