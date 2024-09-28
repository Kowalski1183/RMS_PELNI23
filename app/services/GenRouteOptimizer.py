from fastapi import APIRouter, status, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional
from sqlalchemy import and_
import traceback

from core.endpoint import *
from core.schemas.schemas import GenRouteOptimizer

import requests
import pandas as pd
import numpy as np
from typing import List,Callable,Tuple
from collections import namedtuple
from functools import partial
import time
import datetime
from datetime import datetime
from core.func.scriptall import get_token,Gen_Request_Estimation, Gen_Request_Update_Status
from core.func.general_function import *
from dateutil.relativedelta import relativedelta
router = APIRouter()


@router.post(GENROUTEOPTIMIZER,tags=["Route Optimizer Service"])
def routeOptimizer(request: GenRouteOptimizer):
    try:
        now = datetime.now() + relativedelta(months=1)
        month = now.month
        year = now.year

        formatted_month = str(month).zfill(2)

        start = f"{year}-{formatted_month}-01"
        end = f"{year + 1}-{formatted_month}-01"

        print(start)
        print(end)

        success_count = 0
        failed_count = 0
        failed_requests = []


        idConf, forbidden, df_input = input_df(request.model_dump())
        print(idConf)
        print(forbidden)
        print("__INI DF INPUT__")
        print(df_input.columns)
        
        #EXECUTE GLOBAL VARIABLES

        print("START RETRIEVE DATA FOR OPTIMIZER")
        #master port, exclude forbiddenport
        df_port_all = retrieve_port('001')
        df_port = df_port_all[~df_port_all['id'].isin([forbidden])]
        df_port = cleaned_port(df_port)
        
        # print("__INI DF PORT__")
        # print(df_port.columns)
        
        
        #master ship, include input ship only
        df_ship_all = retrieve_ship('001')
        df_ship = df_ship_all[df_ship_all['id'].isin(df_input.idShip)]
        df_ship=df_ship.fillna(0)
        ship_input=pd.merge(df_ship,df_input, how='inner', left_on=['id'], right_on=['idShip'])
        
        # print("__INI DF SHIP__")
        # print(df_ship.columns)
        
        # print("__INI SHIP INPUT__")
        # print(ship_input.columns)
        
        
        #config price cargo
        # print("__INI DF_PRICECARGOCONFIG__")
        df_pricecargoconfig = retrieve_pricecargoconfig()
        # print(df_pricecargoconfig.columns)
        
        #config low peak
        # print("__INI DF_LOWPEAK__")
        df_lowpeak = retrieve_lowpeak()
        # print(df_lowpeak.columns)

        #config basefare
        # print("__INI DF_BASEFARE__")
        df_basefare = retrieve_basefare()
        # print(df_basefare.columns)
        
        #config cost rule
        # print("__INI DF_RULECOST__")
        df_rulecost = retrieve_rulecost()
        # print(df_rulecost.columns)

        # #price cargo flat
        # print("__INI DF_CARGOFLAT__")
        df_cargoflat = retrieve_cargoflat()
        # print(df_cargoflat.columns)

        # #price adjustment
        # print("__INI DF_ADJUSTMENT__")
        df_adjustment = retrieve_adjustment()
        # print(df_adjustment)

        # #config price pnp
        # print("__INI DF_PRICECONFIG__")
        df_priceconfig = retrieve_priceconfig()
        # print(df_priceconfig.columns)
        
        #master port distance
        df_portdistance = retrieve_portdistance()
        df_new = cleansing_portdistance(df_port,df_portdistance)
        if len(df_new)>len(df_portdistance):
            df_portdistance = copy.deepcopy(df_new)
            
        # #revenue forecast
        # print("__INI DF_REVENUE__")
        df_revenue = merge_hist_pax(start,end)
        # print(df_revenue.columns)
        
        df_lowrev, df_peakrev = separate_rev(df_revenue, df_lowpeak)
        
        print("FINISH RETRIEVE DATA FOR OPTIMIZER")
            
        # Buat ngecek genRouteAlpha succes or not
        success_low = False
        success_peak = False
        
        start = time.time()
        output_low = generate_route_beta_1(
            copy.deepcopy(ship_input), 
            copy.deepcopy(df_port), 
            copy.deepcopy(df_ship), 
            copy.deepcopy(df_portdistance), 
            copy.deepcopy(df_adjustment),
            copy.deepcopy(df_basefare), 
            copy.deepcopy(df_cargoflat), 
            copy.deepcopy(df_lowpeak), 
            copy.deepcopy(df_pricecargoconfig), 
            copy.deepcopy(df_priceconfig), 
            copy.deepcopy(df_lowrev), 
            copy.deepcopy(df_rulecost), 
            season='LOW', 
            npop=15, 
            fitness_limit=2.8, 
            generation_limit=40
        )
        
        header_low = output_low[0]
        est_low = output_low[1].fillna(0)
        cost_low = output_low[2]
        matrix_low = output_low[3]
        end = time.time()
        runningTimeLow = end-start        
        print(f"time spent for regular optimiser: {end-start}s")
        
        if header_low is not None and est_low is not None and cost_low is not None:
            success_low = True
            print(f"Regular optimiser successful. Time spent: {runningTimeLow}s")

        start = time.time()
        output_peak = generate_route_beta_1(
            copy.deepcopy(ship_input), 
            copy.deepcopy(df_port), 
            copy.deepcopy(df_ship), 
            copy.deepcopy(df_portdistance), 
            copy.deepcopy(df_adjustment),
            copy.deepcopy(df_basefare), 
            copy.deepcopy(df_cargoflat), 
            copy.deepcopy(df_lowpeak), 
            copy.deepcopy(df_pricecargoconfig), 
            copy.deepcopy(df_priceconfig), 
            copy.deepcopy(df_peakrev), 
            copy.deepcopy(df_rulecost), 
            season='PEAK', 
            npop=20, 
            fitness_limit=2.8, 
            generation_limit=40
        )
        
        header_peak = output_peak[0]
        est_peak = output_peak[1].fillna(0)
        cost_peak = output_peak[2]
        matrix_peak = output_peak[3]
        end = time.time()
        runningTimePeak = end-start 
        print(f"time spent for peak optimiser: {end-start}s")
        
        if header_peak is not None and est_peak is not None and cost_peak is not None:
            success_peak = True
            print(f"Peak optimiser successful. Time spent: {runningTimePeak}s")
            
        header_all = pd.concat([header_low, header_peak], axis=0).fillna(0).reset_index(drop=True)
        est_all = pd.concat([est_low, est_peak], axis=0).fillna(0).reset_index(drop=True)
        cost_all = pd.concat([cost_low, cost_peak], axis=0).fillna(0).reset_index(drop=True)
        matrix_all = pd.concat([matrix_low, matrix_peak], axis=0).fillna(0).reset_index(drop=True)
    
        # print("header_all")
        # print(header_all.columns)
        # print(header_all.head())
        
        # print("est_all")
        # print(est_all.columns)
        
        # print("cost_all")
        # print(cost_all.columns)
        
        # print("matrix_all")
        # print(matrix_all.columns)
        
        # Define the URLs for the external APIs
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        
        runningTime = round(runningTimeLow + runningTimePeak)
        idConfRoute = idConf
        #CEK BERHASIL ATAU GAGAL SI genRouteAlpha
        if success_low or success_peak:
            #SALAH SATU BERHASIL, PANGGIL SI REQUEST ESTIMATION
            if success_low:
                statusResponse = "HOLD"
            else:
                statusResponse = "HOLD"
                
            #estimation_request_bodies = [] ##buat ngecek hasil request body per header
            # fullRequestBodyEstimation =  Gen_Request_Estimation(header_all, est_all, cost_all, matrix_all, statusResponse)

            total_data = len(header_all)
            current_iteration = 0
            # Process each estimation one by one
            for index, header_row in header_all.iterrows():
                current_iteration += 1
                
                # Calculate progress
                progress = (index + 1) / total_data * 100
               
                # Generate request body for single row
                temp_header = header_all.iloc[[index]]
                temp_est = est_all[(est_all['idConfDetail'] == header_row['idConfDetail']) &
                                (est_all['typeSeason'] == header_row['typeSeason']) &
                                (est_all['option'] == header_row['option'])]

                temp_cost = cost_all[(cost_all['idConfDetail'] == header_row['idConfDetail']) &
                                    (cost_all['typeSeason'] == header_row['typeSeason']) &
                                    (cost_all['option'] == header_row['option'])]

                temp_matrix = matrix_all[(matrix_all['idConfDetail'] == header_row['idConfDetail']) &
                                            (matrix_all['typeSeason'] == header_row['typeSeason']) &
                                            (matrix_all['option'] == header_row['option'])]
                
                single_request_body = Gen_Request_Estimation(temp_header, temp_est, temp_cost, temp_matrix, statusResponse)
                #estimation_request_bodies.append(single_request_body) ##buat ngecek hasil request body per header
                
                # Send the request for single row
                response = requests.put(ESTIMATION_API_URL, json=single_request_body, headers=headers)
                if response.status_code == 200:
                    print(f"Successfully sent estimation for idConfDetail {header_row['idConfDetail']}, typeSeason : {header_row['typeSeason']}, option : {header_row['option']}")
                    success_count += 1
                    # Prepare data for updating status to COMPLETED
                    update_status_data = Gen_Request_Update_Status(idConfRoute, forbidden, status='COMPLETED', runningTime=runningTime, accuracy=0, method='Optimizer', trial=0)
                    
                    # Synchronous HTTP request for updating status
                    update_status_response = requests.patch(CONFIG_UPDATE_STATUS_API, json=update_status_data, headers=headers)
                    print(update_status_response)
                    
                    if update_status_response.status_code == 200:
                        print(f"UPDATE STATUS SUCCESS for idConfDetail {header_row['idConfDetail']}, typeSeason : {header_row['typeSeason']}, option : {header_row['option']} ")
                    else:
                        print(f"UPDATE STATUS FAILED for idConfDetail {header_row['idConfDetail']}, typeSeason : {header_row['typeSeason']}, option : {header_row['option']}, response : {update_status_response.status_code}")
                        print(f"Error response: {update_status_response.text}")
                elif response.status_code == 413:
                    print(f"Payload too large for idConfDetail {header_row['idConfDetail']}")
                    return JSONResponse(
                        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                        content={
                            "status": {
                                "responseCode": str(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE),
                                "responseDesc": "Payload too large, cannot process the estimation.",
                                "responseMessage": "Payload too large, cannot process the estimation."
                            }
                        }
                    )
                else:
                    print("="*50)
                    print(f"Failed to send estimation for idConfDetail {header_row['idConfDetail']}, typeSeason : {header_row['typeSeason']}, option : {header_row['option']}")
                    failed_count += 1
                    
                    # Prepare data for updating status to FAILED
                    update_status_data = Gen_Request_Update_Status(idConfRoute, forbidden, status='FAILED', runningTime=runningTime, accuracy=0, method='Optimizer', trial=0) 
                    
                    # Synchronous HTTP request for updating status
                    update_status_response = requests.patch(CONFIG_UPDATE_STATUS_API, json=update_status_data, headers=headers)
                    if update_status_response.status_code == 200:
                        print(f"UPDATE STATUS to FAILED for idConfDetail {header_row['idConfDetail']}, typeSeason : {header_row['typeSeason']}, option : {header_row['option']} ")
                    else:
                        print(f"Error response: {update_status_response.text}")
                    
                    data_estimation = response.json()
                    error_details = {
                        "idConfDetail": header_row['idConfDetail'],
                        "typeSeason": header_row['typeSeason'],
                        "option": header_row['option'],
                        "error":{
                            "responseCode": data_estimation['status']['responseCode'],
                            "responseDesc": data_estimation['status']['responseDesc'],
                            "responseMessage": "Failed to Create Estimation Table"
                            }
                        }
                    failed_requests.append(error_details)
                    
                print(f"Progress HIT API ESTIMATION: {progress:.2f}% , Data {current_iteration} out of {total_data} data")
                
            
            #KASIH STRUKTUR KALO FAILED REQUEST KOSONG
            if not failed_requests:
                failed_requests.append({
                    "idConfDetail": "",
                    "typeSeason": "",
                    "option": "",
                    "error":{
                        "responseCode": "",
                        "responseDesc": "",
                        "responseMessage": "" 
                    }
                })
   
            # Prepare a summary of successful and failed requests
            summary = {
                "successData": success_count,
                "failedData": failed_count,
                "detailedFailedRequest" : failed_requests
            }
            

            print("OPTIMIZER SUCCESS")
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "status": {
                        "responseCode": str(status.HTTP_200_OK),
                        "responseMessage": "Success!",
                        "responseDesc": "Success to Optimized Route and Update Status",
                        "request" : request.model_dump(),
                        "summaryEstimationData": summary
                    }
                }
            )
            
    except Exception as err:
        token = get_token()  
        headers = {"Authorization": f"Bearer {token}"}
        idConf, forbidden, df_input = input_df(request.model_dump())
        
        update_status_data = Gen_Request_Update_Status(idConf, forbidden, 'FAILED', 0, 0, 'Optimizer', 0)
        
        update_status_response = requests.patch(CONFIG_UPDATE_STATUS_API, json=update_status_data, headers=headers)
        print(update_status_response)
        
        success_count = 0
        failed_count = 0
        failed_requests = []
        
        #KASIH STRUKTUR KALO FAILED REQUEST KOSONG
        if not failed_requests:
            failed_requests.append({
                "idConfDetail": "",
                "typeSeason": "",
                "option": "",
                "error":{
                    "responseCode": "",
                    "responseDesc": "",
                    "responseMessage": "" 
                }
            })
        
        summary = {
            "successData": success_count,
            "failedData": failed_count,
            "detailedFailedRequest" : failed_requests
        }
        
        #Cek Status Hit API update
        if update_status_response.status_code == 200:
            print("Optimizer Failed, Success Update Status to FAILED")
            traceback.print_exc()
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={
                    "status": {
                        "responseCode": str(status.HTTP_500_INTERNAL_SERVER_ERROR),
                        "responseMessage":"Failed to Run Optimizer, Success update status to FAILED",
                        "responseDesc":f"{err}",
                        "request": request.model_dump(),
                        "summaryEstimationData": summary
                    }
                }
            )     
        else:
            print("Optimizer Failed, FAILED Update Status to FAILED")
            data_update_status = update_status_response.json()
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={
                    "status": {
                        "responseCode": str(data_update_status['status']['responseCode']),
                        "responseMessage": "Failed to Run Optimizer, Failed update status to FAILED",
                        "responseDesc":data_update_status['status']['responseDesc'],
                        "request": request.model_dump(),
                        "summaryEstimationData": summary
                    }
                }
            )     