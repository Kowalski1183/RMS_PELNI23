from fastapi import APIRouter, status
from typing import Optional

from core.endpoint import REVENUEPAXDAILY, REVENUEPAXYEARLY
from core.database.config import headers
from core.func.histpaxrevenue.functions import fetch_data_from_url

router = APIRouter()

@router.get(REVENUEPAXYEARLY, tags=["Retrieve Service"])
async def revenue_pax_yearly(org: Optional[str] = None, des: Optional[str] = None, year: Optional[str] = None):

    url = "https://api-dev.pelni.co.id/passenger/revenue/yearly"

    params = {}
    if org:
        params["org"] = org
    if des:
        params["des"] = des
    if year:
        params["year"] = year

    response = await fetch_data_from_url(url=url, headers=headers, params=params)
    
    if response is None or "error" in response :
        return {
                'status': {
                    "responseCode": status.HTTP_404_NOT_FOUND,
                    "responseDesc": "API Pelni Issues",
                    "responseMessage": "Failed to retrieve data "+year+" from Pelni API"
                }
            }
    else:
        data = response['data']
        result = {
            'status': {
                "responseCode": status.HTTP_200_OK,
                "responseDesc": "Success retrieve data",
                "responseMessage": "Success to retrieve data "+year+" from Pelni API"
            },
            "data": data
        }
        return result

@router.get(REVENUEPAXDAILY, tags=["Retrieve Service"])
async def revenue_pax_daily(org: Optional[str] = None, des: Optional[str] = None,
                            start_date: Optional[str] = None, end_date: Optional[str] = None):
    url = "https://api-dev.pelni.co.id/passenger/revenue/daily"

    # parameter
    if org is not None:
        url += f"?org={org}"
    if des is not None:
        separator = "&" if "?" in url else "?"
        url += f"{separator}des={des}"
    if start_date is not None:
        separator = "&" if "?" in url else "?"
        url += f"{separator}start_date={start_date}"
    if end_date is not None:
        separator = "&" if "?" in url else "?"
        url += f"{separator}end_date={end_date}"

    response = await fetch_data_from_url(url=url, headers=headers)

    if response is None or response == 'null' or response['data'] == []:
        return {
                'status': {
                    "responseCode": status.HTTP_404_NOT_FOUND,
                    "responseDesc": "API Pelni Issues",
                    "responseMessage": "Failed to retrieve data "+start_date+"-"+end_date+" from Pelni API"
                }
            }
    elif 'error' in response or '401' in response:
        return {
                'status': {
                    "responseCode": status.HTTP_500_INTERNAL_SERVER_ERROR,
                    "responseDesc": "API Pelni Issues",
                    "responseMessage": "Failed to retrieve data "+start_date+"-"+end_date+" from Pelni API"
                }
            }
    else:
        data = response['data']
        result = {
            'status': {
                "responseCode": status.HTTP_200_OK,
                "responseDesc": "Success retrieve data",
                "responseMessage": "Success to retrieve data "+start_date+"-"+end_date+" from Pelni API"
            },
            "data": data
        }
        return result