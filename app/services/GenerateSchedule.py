from fastapi import APIRouter, status, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
import traceback
import requests
from core.endpoint import CREATESCHEDULE, GENERATESCHEDULE, DATA_CLEANSING, UPDATE_STATUS
import pandas as pd
from core.func.data_preparation_k import *
from core.func.general_function_schedule import *
from core.schemas.schemas import GenerateSchedule
from core.func.scriptall import Gen_Request_Schedule, get_token, convert_df, reqBodyScheduleFailed, Req_Body_Cleanse

router = APIRouter()


@router.post(GENERATESCHEDULE, tags=["Generate Schedule Service"])
async def hitGenerateSchedule(request: GenerateSchedule, bg: BackgroundTasks):
    # await generateSchedule(request)
    bg.add_task(generateSchedule, request)

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "status": {
                "responseCode": str(status.HTTP_200_OK),
                "responseMessage": "Success!",
                "responseDesc": "Success to Create Schedule"
            }
        }
    )


def generateSchedule(request: GenerateSchedule):
    try:
        start = request.parameter.data.periodStart
        end = request.parameter.data.periodEnd
        idTaskManagement = request.parameter.data.idTaskManagement
        print(start)
        print(end)

        # master port
        print("START RETRIEVE DATA FOR GEN SCHEDULE")
        # print("DF_PORT")
        df_port = retrieve_port('001')
        df_port = cleaned_port(df_port)

        # print(df_port.columns)
        # print(df_port.head())

        # #master ship
        # print("DF_SHIP")
        df_ship = retrieve_ship('001')
        # print(df_ship.head())
        # print(df_ship.columns)

        # #master port distance
        # print("DF_PORTDISTANCE")
        df_portdistance = retrieve_portdistance()
        df_new = cleansing_portdistance(df_port, df_portdistance)
        if len(df_new) > len(df_portdistance):
            df_portdistance = copy.deepcopy(df_new)
        # print(df_portdistance.columns)
        # print(df_portdistance.head())

        # #route
        # print("DF_ROUTE")
        df_route = retrieve_rlsroute()
        # print(df_route.columns)
        # print(df_route.head())

        # #route detail
        # print("DF_ROUTEDETAIL")
        df_routedetail = retrieve_rlsroutedetail()
        # print(df_routedetail.columns)
        # print(df_routedetail.head())

        # #config price cargo
        # print("DF_PRICECARGOCONFIG")
        df_pricecargoconfig = retrieve_pricecargoconfig()
        # print(df_pricecargoconfig.columns)
        # print(df_pricecargoconfig.head())

        # #config low peak
        # print("DF_LOWPEAK")
        df_lowpeak = retrieve_lowpeak()
        # print(df_lowpeak.columns)
        # print(df_lowpeak.head())

        # #config basefare
        # print("DF_BASEFARE")
        df_basefare = retrieve_basefare()
        # print(df_basefare.columns)
        # print(df_basefare.head())

        # #config cost rule
        # print("DF_RULECOST")
        df_rulecost = retrieve_rulecost()
        # print(df_rulecost.columns)
        # print(df_rulecost.head())

        # #price cargo flat
        # print("DF_CARGOFLAT")
        df_cargoflat = retrieve_cargoflat()
        # print(df_cargoflat.columns)
        # print(df_cargoflat.head())

        # #price adjustment
        # print("DF_ADJUSTMENT")
        df_adjustment = retrieve_adjustment()

        # #config price pnp
        # print("DF_PRICECONFIG")
        df_priceconfig = retrieve_priceconfig()
        # print(df_priceconfig.columns)
        # print(df_priceconfig.head())

        # #master tide
        # print("DF_TIDE")
        df_tide = retrieve_tide()
        # print(df_tide.columns)
        # print(df_tide.head())

        # # maintenance schedule
        # print("DF_MAINTENANCE")
        df_maintenance = retrieve_maintenance()
        # print(df_maintenance.columns)
        # print(df_maintenance.head())

        # # revenue forecasted
        # print("DF_REVENUE")
        df_revenue = merge_hist_pax(start, end)
        # print(df_revenue.columns)
        # print(df_revenue.head())

        print("FINISH RETRIEVE DATA FOR GEN SCHEDULE")

        output_dfs = generate_schedule_beta_1(start, end, df_route, df_routedetail, df_ship, df_port, df_portdistance, df_revenue, df_rulecost,
                                              df_cargoflat, df_pricecargoconfig, df_lowpeak, df_basefare, df_priceconfig, df_adjustment, df_maintenance, df_tide)
        all_trip = output_dfs[0]
        all_schedule = output_dfs[1]
        all_revenue = output_dfs[2]
        all_cost = output_dfs[3]
        all_matrix = output_dfs[4]

        all_schedule
        # print("DF ALL_SCHEDULE")
        all_schedule['departure_time'] = pd.to_datetime(
            all_schedule['departure_time'])
        all_schedule['arrival_time'] = pd.to_datetime(
            all_schedule['arrival_time'])
        all_schedule['departure_time'] = all_schedule['departure_time'].dt.strftime(
            '%Y-%m-%d %H:%M:%S')
        all_schedule['arrival_time'] = all_schedule['arrival_time'].dt.strftime(
            '%Y-%m-%d %H:%M:%S')
        all_schedule = all_schedule.fillna(0)
        # print(all_schedule.columns)
        # print(all_schedule.head())

        # print("DF ALL_TRIP")
        all_trip['departuretime'] = pd.to_datetime(
            all_trip['departuretime'])
        all_trip['arrivaltime'] = pd.to_datetime(
            all_trip['arrivaltime'])
        all_trip['departuretime'] = all_trip['departuretime'].dt.strftime(
            '%Y-%m-%d %H:%M:%S')
        all_trip['arrivaltime'] = all_trip['arrivaltime'].dt.strftime(
            '%Y-%m-%d %H:%M:%S')
        all_trip = all_trip.fillna(0)
        # print(all_trip.columns)
        # print(all_trip.head())

        # print("DF ALL_COST")
        all_cost = all_cost.fillna(0)
        # print(all_cost.columns)
        # print(all_cost.head())

        # print("DF ALL_revenue")
        all_revenue = all_revenue.fillna(0)
        # print(all_revenue.columns)
        # print(all_revenue.head())

        # print("DF ALL_MATRIX")
        all_matrix = all_matrix.fillna(0)
        # print(all_matrix.columns)
        # print(all_matrix.head())

        # Apply the conversion function to each DataFrame
        all_trip = convert_df(all_trip)
        all_schedule = convert_df(all_schedule)
        all_cost = convert_df(all_cost)
        all_revenue = convert_df(all_revenue)
        all_matrix = convert_df(all_matrix)

        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}

        schedule_request_bodies = []  # buat ngecek hasil request body per trip
        failed_requests = []
        success_count = 0
        failed_count = 0
        # fullRequestBodySchedule = Gen_Request_Schedule(
        #         idTaskManagement, start, end, all_trip,
        #         all_schedule, all_cost, all_revenue, all_matrix
        #     )

        # Hit API Data Cleansing
        reqBodyCleansing = Req_Body_Cleanse(start, end, idTaskManagement)
        responseCleansing = requests.post(
            DATA_CLEANSING, json=reqBodyCleansing, headers=headers)

        # cek status response API Cleansing
        if responseCleansing.status_code != 200:
            print("DATA CLEANSING FAILED")
            data_responseCleansing = responseCleansing.json()
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={
                    "status": {
                        "responseCode": str(data_responseCleansing['status']['responseCode']),
                        "responseMessage": data_responseCleansing['status']['responseMessage'],
                        "responseDesc": data_responseCleansing['status']['responseDesc']
                    }
                }
            )
        else:
            print("DATA CLEANSING SUCCESS")

        total_data = len(all_trip)
        current_iteration = 0

        if total_data == 0:
            request_body_json_update = {
                "parameter": {
                    "data": {
                        "id": idTaskManagement,
                        "status": "FAILED"
                    }
                }
            }

            response_Schedule = requests.patch(
                UPDATE_STATUS, json=request_body_json_update, headers=headers)
            print("data is empty")
            print("Generate Schedule Terminated")
        else :
            # Loop HIT API
            for index, trip_row in all_trip.iterrows():
                current_iteration += 1

                # Calculate progress
                progress = (index + 1) / total_data * 100

                # TAKE DATA ONLY BASED FILTER
                trip_idrls = trip_row['idrls']
                trip_voyage = trip_row['voyage']

                single_schedule = all_schedule[(all_schedule['idrls'] == trip_idrls) & (
                    all_schedule['voyage'] == trip_voyage)].reset_index(drop=True)
                single_cost = all_cost[(all_cost['idrls'] == trip_idrls) & (
                    all_cost['voyage'] == trip_voyage)].reset_index(drop=True)
                single_revenue = all_revenue[(all_revenue['idrls'] == trip_idrls) & (
                    all_revenue['voyage'] == trip_voyage)].reset_index(drop=True)
                single_matrix = all_matrix[(all_matrix['idrls'] == trip_idrls) & (
                    all_matrix['voyage'] == trip_voyage)].reset_index(drop=True)

                # GENERATE REQUEST BODY
                request_body_schedule = Gen_Request_Schedule(
                    idTaskManagement, start, end, all_trip.iloc[[index]],
                    single_schedule, single_cost, single_revenue, single_matrix
                )
                print("Make Generate Schedule Request")

                # Serialize mapping
                request_body_json = request_body_schedule.json() if hasattr(
                    request_body_schedule, 'json') else request_body_schedule
                # buat ngecek hasil request body per trip
                schedule_request_bodies.append(request_body_json)
                print("Generate Schedule Request")
                response_Schedule = requests.post(
                    CREATESCHEDULE, json=request_body_json, headers=headers)
                

                if response_Schedule.status_code == 200:
                    print("CREATE SCHEDULE BERHASIL")
                    success_count += 1
                    print(
                        f"Successfully sent schedule for idShip: {trip_row['idship']}, voyage: {trip_row['voyage']}, flagSeason: {trip_row['season']}")
                else:
                    print("CREATE SCHEDULE GAGAL")
                    print(
                        f"Failed sent schedule for idShip: {trip_row['idship']}, voyage: {trip_row['voyage']}, flagSeason: {trip_row['season']}")
                    failed_count += 1
                    data_response_schedule = response_Schedule.json()
                    error_details = {
                        "idShip": trip_row['idship'],
                        "voyage": trip_row['voyage'],
                        "flagSeason": trip_row['season'],
                        "error": {
                            "responseCode": str(data_response_schedule['status']['responseCode']),
                            "responseDesc": data_response_schedule['status']['responseDesc'],
                            "responseMessage": "Failed to Create Schedule Table"
                        }
                    }
                    failed_requests.append(error_details)

                print(
                    f"Progress HIT API schedule: {progress:.2f}% , Data {current_iteration} out of {total_data} data")

            request_body_json_update = {
                "parameter": {
                    "data": {
                        "id": idTaskManagement,
                        "status": "FINISH"
                    }
                }
            }

            response_Schedule = requests.patch(
                UPDATE_STATUS, json=request_body_json_update, headers=headers)

            print("Generate Schedule Successfully")

        # if not failed_requests:
        #     failed_requests.append({
        #         "idShip": "",
        #         "voyage": "",
        #         "flagSeason": "",
        #         "error": {
        #             "responseCode": "",
        #             "responseDesc": "",
        #             "responseMessage": ""
        #         }
        #     })

        # # If all trips were sent successfully
        # summary = {
        #     "successData": success_count,
        #     "failedData": failed_count,
        #     "detailedFailedRequest": failed_requests
        # }

        # return JSONResponse(
        #     status_code=status.HTTP_200_OK,
        #     content={
        #         "status": {
        #             "responseCode": str(status.HTTP_200_OK),
        #             "responseMessage": "Success!",
        #             "responseDesc": "Success to Create Schedule",
        #             "request": request.model_dump(),
        #             "summaryScheduleData": summary
        #         }
        #     }
        # )

    except Exception as e:
        # success_count = 0
        # failed_count = 0
        # failed_requests = []

        # if not failed_requests:
        #     failed_requests.append({
        #         "idShip": "",
        #         "voyage": "",
        #         "flagSeason": "",
        #         "error": {
        #             "responseCode": "",
        #             "responseDesc": "",
        #             "responseMessage": ""
        #         }
        #     })

        request_body_json_update = {
            "parameter": {
                "data": {
                    "id": idTaskManagement,
                    "status": "FAILED"
                }
            }
        }

        response_Schedule = requests.patch(
            UPDATE_STATUS, json=request_body_json_update, headers=headers)

        # summary = {
        #     "successData": success_count,
        #     "failedData": failed_count,
        #     "detailedFailedRequest": failed_requests
        # }

        print("CREATE SCHEDULE GAGAL")
        print(f"Error encountered : {e}")
        traceback.print_exc()

        # return JSONResponse(
        #     status_code=status.HTTP_400_BAD_REQUEST,
        #     content={
        #         "status": {
        #             "responseCode": str(status.HTTP_400_BAD_REQUEST),
        #             "responseMessage": "Create Schedule Gagal",
        #             "responseDesc": f"{e}",
        #             "request": request.model_dump(),
        #             "summaryScheduleData": summary
        #         }
        #     }
        # )
