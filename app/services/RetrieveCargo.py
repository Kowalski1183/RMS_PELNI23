from fastapi import APIRouter, status
from typing import Optional

from core.endpoint import REVENUECARGODAILY, REVENUECARGOYEARLY
from core.database.config import headers
from core.func.histpaxrevenue.functions import fetch_data_from_url


router = APIRouter()

@router.get(REVENUECARGOYEARLY, tags=["Retrieve Service"])
async def revenue_cargo_yearly(org: Optional[str] = None,
                               des: Optional[str] = None,
                               year: Optional[str] = None):

    url = "https://api-dev.pelni.co.id/master/v1/cargo/revenue/yearly"

    if org is not None:
        url += f"?org={org}"

    if des is not None:
        sepator = "&" if "?" in url else "?"
        url += f"{sepator}des={des}"

    if year is not None:
        sep = "&" if "?" in url else "?"
        url += f"{sep}year={year}"

    respone = await fetch_data_from_url(url=url, headers=headers)
    
    if respone is None or "error" in respone:
        return {
                'status': {
                    "responseCode": status.HTTP_404_NOT_FOUND,
                    "responseDesc": "API Pelni Issues",
                    "responseMessage": "Failed to retrieve data "+year+" from Pelni API"
                }
            }
    else:
        data = respone["data"]
        result = {
            'status': {
                "responseCode": status.HTTP_200_OK,
                "responseDesc": "Success retrieve data",
                "responseMessage": "Succes to retrieve data "+year+" from Pelni API"
            },
            "data": data
        }
        return result

@router.get(REVENUECARGODAILY, tags=["Retrieve Service"])
async def revenue_cargo_daily(org: Optional[str] = None, des: Optional[str] = None,
                              start_date: Optional[str] = None, end_date: Optional[str] = None):
    url = "https://api-dev.pelni.co.id/master/v1/cargo/revenue/daily"

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
    elif 'error' in response:
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
                "responseMessage": "Succes to retrieve data "+start_date+"-"+end_date+" from Pelni API"
            },
            "data": data
        }
        return result