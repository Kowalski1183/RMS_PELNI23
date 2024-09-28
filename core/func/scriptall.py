import pandas as pd
import numpy as np
import math
import requests
from fastapi import HTTPException
import httpx
from httpx import Timeout

from typing import List

from core.endpoint import *
from core.func.general_function import *
from core.func.data_preparation import *
from .calculator import calculate_prediction
from random import choices, randint, randrange, random, shuffle


def Gen_Request_Estimation(header_all, est_all, cost_all, all_matrix, status):
    request_body = {
        "parameter": {
            "data": {
                "estRouteList": []
            }
        }
    }

    for _, row in header_all.iterrows():
        est_route = {
            "totDistance": float(row['totDistance']),
            "totCost": float(row['totSubcost']),
            "typeSeason": str(row['typeSeason']),
            "option": str(row['option']),
            "status": status,
            "confRouteDetail": {
                "idConfRouteDetail": str(row['idConfDetail'])
            },
            "commision": float(row['commision']),
            "totSail": float(row['totSail']),
            "totBerth": float(row['totBerth']),
            "totRevenue": float(row['totRevenue']),
            "avgFactor": float(row['avgFactor']),
            "avgOnboard": float(row['avgOnboard']),
            "totTotal": float(row['totTotal']),
            "estRouteDetail": [],
            "estCostDetail": [],
            "estMatrixTotal": []
        }

        matching_est_rows = est_all[(est_all['idConfDetail'] == row['idConfDetail']) & (
            est_all['typeSeason'] == row['typeSeason']) & (est_all['option'] == row['option'])]

        for _, est_row in matching_est_rows.iterrows():
            factor_value = est_row['factor']
            type_value = est_row['type']

            # pengkondisian buat factor -> inf
            if math.isfinite(factor_value):
                factor_value = float(est_row['factor'])
            else:
                factor_value = 0

            # pengkondisian buat VALUE TYPE
            type_mapping = {
                "DRY CONTAINER": "DRY_CONTAINER",
                "REEFER CONTAINER": "REEFER_CONTAINER",
                "GENERAL CARGO": "GENERAL_CARGO",
            }

            type_value = type_mapping.get(type_value, str(est_row['type']))

            est_detail = {
                "factor": factor_value,
                "estUp": float(est_row['estUp']),
                "estDown": float(est_row['estDown']),
                "estRevenue": float(est_row['estRevenue']),
                "ruas": int(est_row['ruas']),
                "type": type_value,
                "estOnboard": float(est_row['estOnboard']),
                "estTotal": float(est_row['total']),
                "portDistance": float(est_row['distance']),
                "portOrigin": {
                    "idPort": str(est_row['portOrigin'])
                },
            }
            est_route["estRouteDetail"].append(est_detail)

        matching_cost_rows = cost_all[(cost_all['idConfDetail'] == row['idConfDetail']) & (
            cost_all['typeSeason'] == row['typeSeason']) & (cost_all['option'] == row['option'])]

        for _, cost_row in matching_cost_rows.iterrows():
            cost_detail = {
                "ruleCost": {
                    "idRuleCost": str(cost_row['idrulecost'])
                },
                "npax": float(cost_row['npax']),
                "sailingTime": float(cost_row['sailingtime']),
                "berthingTime": float(cost_row['berthingtime']),
                "subtotalCost": float(cost_row['cost'])
            }
            est_route["estCostDetail"].append(cost_detail)

        # Adding matrix data
        matching_matrix_rows = all_matrix[(all_matrix['idConfDetail'] == row['idConfDetail']) & (
            all_matrix['typeSeason'] == row['typeSeason']) & (all_matrix['option'] == row['option'])]

        for _, matrix_row in matching_matrix_rows.iterrows():
            matrix_total = {
                "portOrigin": {
                    "idPort": str(matrix_row['portOrigin'])
                },
                "portDestination": {
                    "idPort": str(matrix_row['portDestination'])
                },
                "type": str(matrix_row['type']),
                "total": float(matrix_row['total']),
                "ruasOrigin": matrix_row['ruas_origin'],
                "ruasDestination": matrix_row['ruas_destination'],
                "revenue": matrix_row['revenue']
            }
            est_route["estMatrixTotal"].append(matrix_total)

        request_body["parameter"]["data"]["estRouteList"].append(est_route)

    return request_body


def Gen_Request_Update_Status(idConfRoute, forbiddenPort, status, runningTime, accuracy=0, method="string", trial=0):
    update_status_data = {
        "parameter": {
            "data": {
                "idConfRoute": idConfRoute,
                "forbiddenPort": forbiddenPort,
                "status": status,
                "runningTime": runningTime,
                "accuracy": accuracy,
                "method": method,
                "trial": trial
            }
        }
    }

    return update_status_data


def get_token():
    url_api = USER_LOGIN

    request_body = {
        "paging": {
            "page": 0,
            "limit": 0,
            "totalpage": 0,
            "totalrecord": 0,
            "totalPage": 0,
            "totalRecord": 0
        },
        "parameter": {
            "customQuery": [
                {
                    "value": [{}],
                    "string": "string",
                    "connector": "AND"
                }
            ],
            "sort": {
                "additionalProp1": "string",
                "additionalProp2": "string",
                "additionalProp3": "string"
            },
            "columns": ["string"],
            "data": {
                "username": "Admin",
                "password": "pwd123"
            },
            "criteriaType": "string",
            "criteria": {
                "additionalProp1": "string",
                "additionalProp2": "string",
                "additionalProp3": "string"
            },
            "filter": {
                "additionalProp1": {},
                "additionalProp2": {},
                "additionalProp3": {}
            },
            "between": {
                "additionalProp1": {
                    "from": {},
                    "to": {}
                },
                "additionalProp2": {
                    "from": {},
                    "to": {}
                },
                "additionalProp3": {
                    "from": {},
                    "to": {}
                }
            },
            "isDistinct": True
        }
    }

    try:
        response = requests.post(url_api, json=request_body)
        # Raises an HTTPError if the HTTP request returned an unsuccessful status code
        response.raise_for_status()
        return response.json()["result"]["token"]
    except requests.HTTPError as e:
        print(f"HTTP error status code: {e.response.status_code}")
        print(f"HTTP error response: {e.response.text}")
        raise
    except requests.RequestException as e:
        print(f"Request error occurred: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise


def Gen_Request_Schedule(idTaskManagement, start, end, df_all_trip, df_all_schedule, df_all_cost, df_all_revenue, df_all_matrix):
    data_structure = {
        "parameter": {
            "data": {
                "idTaskManagement": idTaskManagement,
                "start": start,
                "end": end,
                "tripList": []
            }
        }
    }

    for _, trip_row in df_all_trip.iterrows():
        trip = {
            "ship": {"idShip": trip_row['idship']},
            "originPort": {"idPort": trip_row['origin']},
            "destinationPort": {"idPort": trip_row['destination']},
            "flagSeason": trip_row['season'],
            "departureDatetime": trip_row['departuretime'],
            "arrivalDatetime": trip_row['arrivaltime'],
            "voyage": trip_row['voyage'],
            "scheduleDetail": [],
            "scdCost": [],
            "schMatrixTotal": []
        }

        for _, schedule_row in df_all_schedule[(df_all_schedule['idrls'] == trip_row['idrls']) & (df_all_schedule['voyage'] == trip_row['voyage'])].iterrows():
            relevant_revenue = df_all_revenue[(df_all_revenue['voyage'] == schedule_row['voyage']) & (
                df_all_revenue['idrls'] == trip_row['idrls']) & (df_all_revenue['ruas'] == schedule_row['ruas'])]

            revenue_list = []

            for _, revenue_row in relevant_revenue.iterrows():
                revenue_data = revenue_row.to_dict()

                for key, value in revenue_data.items():
                    if isinstance(value, np.number):
                        revenue_data[key] = value.item()

                revenue_list.append({
                    "portDistance": revenue_data.get('distance', 0),
                    "factor": revenue_data['factor'],
                    "up": revenue_data['up'],
                    "down": revenue_data['down'],
                    "onboard": revenue_data['onboard'],
                    "total": revenue_data['total'],
                    "revenue": revenue_data['revenue'],
                    "type": revenue_data['type']
                })

            detail = {
                "port": {"idPort": schedule_row['idport']},
                "departureDatetime": schedule_row['departure_time'],
                "arrivalDatetime": schedule_row['arrival_time'],
                "ruas": schedule_row['ruas'],
                "scdRevenueList": revenue_list
            }
            trip["scheduleDetail"].append(detail)

        for _, cost_row in df_all_cost[(df_all_cost['idrls'] == trip_row['idrls']) & (df_all_cost['voyage'] == trip_row['voyage'])].iterrows():
            cost = {
                "ruleCost": {"idRuleCost": cost_row['idrulecost']},
                "pax": cost_row['pax'],
                "day": cost_row['day'],
                "cost": cost_row['cost']
            }
            trip["scdCost"].append(cost)

        for _, matrix_row in df_all_matrix[(df_all_matrix['idrls'] == trip_row['idrls']) & (df_all_matrix['voyage'] == trip_row['voyage'])].iterrows():
            matrix_total = {
                "portOrigin": {"idPort": matrix_row['origin']},
                "portDestination": {"idPort": matrix_row['destination']},
                "type": matrix_row['type'],
                "total": matrix_row['total'],
                "ruasOrigin": matrix_row['ruas_origin'],
                "ruasDestination": matrix_row['ruas_destination'],
                "revenue": matrix_row['revenue']
            }
            trip["schMatrixTotal"].append(matrix_total)

        data_structure["parameter"]["data"]["tripList"].append(trip)

    return data_structure


def reqBodyScheduleFailed(idTaskManagement, start, end):
    data = {
        "parameter": {
            "data": {
                "idTaskManagement": idTaskManagement,
                "start": start,
                "end": end,
                "tripList": [
                    {
                        "ship": {"idShip": None},
                        "originPort": {"idPort": None},
                        "destinationPort": {"idPort": None},
                        "flagSeason": None,
                        "departureDatetime": None,
                        "arrivalDatetime": None,
                        "voyage": None,
                        "scheduleDetail": [
                            {
                                "port": {"idPort": None},
                                "departureDatetime": None,
                                "arrivalDatetime": None,
                                "ruas": None,
                                "scdRevenueList": [
                                    {
                                        'portDistance': 0,
                                        "factor": 0,
                                        "up": 0,
                                        "down": 0,
                                        "onboard": 0,
                                        "total": 0,
                                        "revenue": 0,
                                        "type": None
                                    }
                                ]
                            }
                        ],
                        "scdCost": [
                            {
                                "ruleCost": {"idRuleCost": None},
                                "pax": 0,
                                "day": 0,
                                "cost": 0
                            }
                        ],
                        "schMatrixTotal": [
                            {
                                "portOrigin": {"idPort": None},
                                "portDestination": {"idPort": None},
                                "type": None,
                                "total": 0
                            }
                        ]
                    }
                ]
            }
        }
    }
    return data


def convert_df(df):
    # Convert int64 columns to int
    int_cols = df.select_dtypes(include=['int64', 'int32']).columns
    df[int_cols] = df[int_cols].applymap(int)

    # Convert float64 columns to float and replace non-finite values
    float_cols = df.select_dtypes(include=['float64', 'float32']).columns
    df[float_cols] = df[float_cols].applymap(
        lambda x: float(x) if np.isfinite(x) else None)

    return df


def check_numpy_types(data, parent_key=''):
    if isinstance(data, dict):
        for k, v in data.items():
            p_key = f"{parent_key}.{k}" if parent_key else k
            check_numpy_types(v, p_key)
    elif isinstance(data, list):
        for i, item in enumerate(data):
            p_key = f"{parent_key}[{i}]"
            check_numpy_types(item, p_key)
    else:
        if isinstance(data, np.generic):
            print(f"Found numpy type at '{parent_key}': {type(data)}")


def Req_Body_Cleanse(start, end, idTaskManagement):
    body = {
        "parameter":
            {"data":
                {"periodStart": start,
                 "periodEnd": end,
                 "idTaskManagement": idTaskManagement
                 }
             }
    }
    return body
