from fastapi import status
from core.schemas.schemas import *
from core.endpoint import ESTREVENUEFACTOR
# from core.func.data_preparation_k import *
from core.func.general_function import *
import pandas as pd
from core.models.modelsrms import HistPaxRevenueRms, MasterPortRms, MasterShipRms
from core.database.sessions import SessionLocal
import json

from fastapi import APIRouter, Depends

router = APIRouter()

@router.post(ESTREVENUEFACTOR,tags=["Calculator Service"])
def revenueDetail(request: EstRevenueFactorRequest):
    db = SessionLocal()
    if request.season=='REGULAR':
        seasons='LOW'
    else:
        seasons=request.season

    ship = db.query(MasterShipRms.id_ship).filter(MasterShipRms.id_ship.like(f"%{request.idShip}%"))
    ship_result = ship.first()
    if ship_result is None:
        response_dict = {
            'status': {
                "responseCode": str(status.HTTP_404_NOT_FOUND),
                "responseDesc": "ship from request not found/empty",
                "responseMessage": "data from request not found/empty in database"
            }
        }
    elif ship_result != None:
        x=0
        for row in request.idPort:
            port = db.query(MasterPortRms.id_port).filter(MasterPortRms.id_port.like(f"%{row}%"))
            port_result = port.first()
            if port_result != None:
                x+1
        t = len(request.idPort)-x
        if len(request.idPort) != t :
            response_dict = {
                'status': {
                    "responseCode": str(status.HTTP_404_NOT_FOUND),
                    "responseDesc": "port from request list not found/empty",
                    "responseMessage": "data from request not found/empty in database"
                }
             }      
        else :
            df_ship = retrieve_ship('001')
            df_cargoflat = retrieve_cargoflat()
            df_pricecargoconfig = retrieve_pricecargoconfig() 
            df_lowpeak = retrieve_lowpeak() 
            df_basefare = retrieve_basefare()
            df_priceconfig = retrieve_priceconfig()
            df_adjustment = retrieve_adjustment()
            df_revenue = retrieve_revenue(2) 
            df_portdistance = retrieve_portdistance()

            dfRevLow, dfRevPeak = separate_rev(df_revenue=df_revenue, df_lowpeak=df_lowpeak)
            
            if seasons == "LOW":
                dfRev = dfRevLow
            else:
                dfRev = dfRevPeak

            prediction = calculate_prediction(request.idPort,
                                              df_rev=dfRev)
            cd_all = covered_demand(prediction, seasons)
            coverage = cal_factor(request.idShip, request.idPort, cd_all, df_ship)

            revenue = cal_revenue(request.idPort, 
                                        None,
                                        cd_all,
                                        df_cargoflat, 
                                        df_pricecargoconfig, 
                                        df_lowpeak, 
                                        df_basefare, 
                                        df_priceconfig, 
                                        df_adjustment,
                                        df_portdistance,
                                        season = seasons)

            revenue = revenue.fillna(0)

            merged1 = pd.merge(revenue,coverage, how='inner', left_on=['origin','type','ruas'], right_on=['port','type','ruas'])
            merged2 = pd.merge(coverage,revenue, how='outer', left_on=['port','ruas','type'], right_on=['origin','ruas','type']).drop(columns = ['origin'])

            if len(merged2)>0:
                typelist = list(revenue['type'].unique())
                maxruas = copy.deepcopy(max(merged2['ruas']))
                merged2 = merged2[(merged2['ruas']==maxruas)&(merged2['type'].isin(typelist))]
                merged_df = pd.concat([merged1,merged2], ignore_index=True)
                merged_df = merged_df.drop(columns=['origin'])
                merged_df = merged_df.fillna(0)
            else:
                merged_df = merged1.drop(columns=['origin'])
                
            merged_df.rename(columns={
                'in': 'est_up',
                'out': 'est_out',
                'onboard': 'est_onboard',
                'total': 'est_total',
                'revenue': 'est_revenue',
                'coverage': 'est_factor'
            }, inplace=True)

            # Add the 'ship' column from the request
            merged_df['ship'] = request.idShip

            # Convert 'int64' columns to regular integers
            merged_df['ruas'] = merged_df['ruas'].astype(int)
            merged_df['est_up'] = merged_df['est_up'].astype(int)
            merged_df['est_out'] = merged_df['est_out'].astype(int)
            merged_df['est_onboard'] = merged_df['est_onboard'].astype(int)

            # Reorder columns
            merged_df = merged_df[['ship', 'port', 'ruas', 'type','distance', 'est_total', 'est_revenue', 'est_up', 'est_out', 'est_onboard', 'est_factor']]

            # Convert the DataFrame to a list of dictionaries
            merged_dict_list = merged_df.to_dict(orient='records')
            
            # Calculate the total and revenue
            total_revenue_dict = {
                'factor':round(merged_df['est_factor'].sum(),2),
                'onboard':int(merged_df['est_onboard'].sum()),
                'distance':merged_df['distance'].sum(),
                "total": merged_df['est_total'].sum(),
                "revenue": merged_df['est_revenue'].sum(),                
            }

            # Create the response dictionary
            response_dict = {
                "status": {
                    "responseCode": status.HTTP_200_OK,
                    "responseDesc": "Success",
                    "responseMessage": "Success fetching data!"
                },
                "result": {
                    "revenueDetail": merged_dict_list,
                    "totalRevenue": total_revenue_dict
                }
            }
    return response_dict