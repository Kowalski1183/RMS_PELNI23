from core.endpoint import ESTCOSTREVENUEFACTOR
from core.schemas.schemas import EstCostRevenueFactorRequest
from fastapi import APIRouter, status
from core.func.costrevenuefactor.functions import cal_cost_revenue_factor
import json
import pandas as pd

router = APIRouter()


@router.post(ESTCOSTREVENUEFACTOR, tags=["Calculator Service"])
async def cal_estcost_revenue_factor(req: EstCostRevenueFactorRequest):
    cost, revenue = cal_cost_revenue_factor(req)
    if revenue == None:
        result = {
            'status': {
                "responseCode": str(status.HTTP_404_NOT_FOUND),
                "responseDesc": "data from request empty",
                "responseMessage": "data from request empty revenue result []"
            }
        }
    else:
        totRevenuePax = 0
        totRevenueCargo = 0
        for i, data in enumerate(revenue['revenueDetail']):
            if data['type'] == 'PASSENGER':
                totRevenuePax = totRevenuePax+data['estRevenue']
            else:
                totRevenueCargo = totRevenueCargo+data['estRevenue']

        countRevenue = len(revenue['revenueDetail'])
        cost_data = cost['data']

        def handle_non_serializable(obj):
            if isinstance(obj, pd.DataFrame):
                return obj.to_dict(orient='records')
            # Add additional checks and conversions for other non-serializable types if needed
            raise TypeError(
                f"Object of type {type(obj)} is not JSON serializable")
        
        revenue_dict = {
            "totSubcost": cost['totalCost'],
            "avgFactor": round(revenue['totalRevenue']['factor']/countRevenue,2),
            "totDistance": revenue['totalRevenue']['distance'],
            "totOnboard": revenue['totalRevenue']['onboard'],
            "avgOnboard": round(revenue['totalRevenue']['onboard']/countRevenue, 2),
            "totTotal": revenue['totalRevenue']['total'],
            "totRevenue": revenue['totalRevenue']['revenue'],
            "totRevenuePax": round(totRevenuePax, 2),
            "totRevenueCargo": round(totRevenueCargo, 2),
            "commision": cost['comDays'],
            'totSail': cost['totSail'],
            'totBerth': cost['totBerth'],
            "estMatrixTotal": revenue['estMatrixTotal'],
            "estRouteDetail": revenue['revenueDetail'],
            "cost": cost_data
        }

        # revenue_json = json.dumps(
        #     revenue_dict, indent=2, default=handle_non_serializable)

        if revenue_dict != None:
            responeStatus = status.HTTP_200_OK
            responeDesc = "Success calculating cost"
            responeMessage = "Success calculating cost"
        else:
            responeStatus = status.HTTP_400_BAD_REQUEST
            responeDesc = "Calculating cost has failed!"
            responeMessage = "Calculating cost has failed!"

        result = {
            'status': {
                "responseCode": str(responeStatus),
                "responseDesc": responeDesc,
                "responseMessage": responeMessage
            },
            'result':
                revenue_dict
        }
    return result
