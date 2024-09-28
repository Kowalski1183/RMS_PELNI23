from fastapi import APIRouter, status
from sqlalchemy import and_, extract, func
from datetime import datetime

from core.models.models import HistPaxRevenue
from core.models.modelsrms import HistPaxRevenueRms
from core.endpoint import INSERTHISTPAXREVENUESTG, INSERTHISTPAXREVENUERMS
from core.database.sessions import SessionLocal 
from core.func.histpaxrevenue.functions import insert_data_to_hist_revenue_stg, insert_data_to_hist_revenue_rms
from .RetrieveCargo import revenue_cargo_daily
from .RetrievePax import revenue_pax_daily

router = APIRouter()

# @router.post(INSERTHISTPAXREVENUESTG, tags=["Insert Service"])
async def insert_hist_pax_revenue_stg(start_date: str, end_date: str): 
    db = SessionLocal()
    
    end_year = end_date.split('-')[0]
    end_month = end_date.split('-')[1]

    end_date_query = db.query(HistPaxRevenue.departure_date).filter(
        and_(
            extract('year', HistPaxRevenue.departure_date) == end_year,
            extract('month', HistPaxRevenue.departure_date) == end_month,
            HistPaxRevenue.type_rev == 0
        )
    )

    date_result = [result[0] for result in end_date_query.all()]
    start_date_c = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_date_c = datetime.strptime(end_date, '%Y-%m-%d').date()
    print(start_date_c in date_result)
    print(end_date_c in date_result)
    if start_date_c in date_result and end_date_c in date_result:
        print("Data from range "+start_date+" - "+end_date+" is found")
        return {
            'status': {
                "responseCode": status.HTTP_200_OK,
                "responseDesc": "Data already exist",
                "responseMessage": "Data cargo & pax from "+start_date+" to "+end_date+" already exist"
            }
        }
    else:
        print("Data from range "+start_date+" - "+end_date+" not found in database")
        print("Continue Insert Data")
        pax = await revenue_pax_daily(None, None, start_date, end_date)
        cargo = await revenue_cargo_daily("", "", start_date, end_date)
        
        if pax['status']['responseCode']==500 & cargo['status']['responseCode']==500:
            return {
                'status': {
                    "responseCode": status.HTTP_500_INTERNAL_SERVER_ERROR,
                    "responseDesc": "Internal Server Error",
                    "responseMessage": "Failed to insert cargo & pax "+start_date+"-"+end_date+"."
                }
            }
        elif pax['status']['responseCode']==404 & cargo['status']['responseCode']==404:
            return {
                'status': {
                    "responseCode": status.HTTP_404_NOT_FOUND,
                    "responseDesc": "Data not found",
                    "responseMessage": "Data cargo & pax "+start_date+"-"+end_date+" Not Found"
                }
            }
        else:
            if pax['status']['responseCode']==404:
                print("Pax data is empty, Continue insert data cargo")
                await insert_data_to_hist_revenue_stg(cargo)
            elif cargo['status']['responseCode']==404:
                print("Cargo data is empty, Continue insert data pax")
                await insert_data_to_hist_revenue_stg(pax)
            else:
                print("Inserting data pax and cargo")
                await insert_data_to_hist_revenue_stg(cargo)
                await insert_data_to_hist_revenue_stg(pax) 

            return {
                'status': {
                    "responseCode": status.HTTP_200_OK,
                    "responseDesc": "Success",
                    "responseMessage": "Succes, data pax & cargo from "+start_date+" to "+end_date+" is inserted"
                }
            }



@router.post(INSERTHISTPAXREVENUERMS, tags=["Insert Service"])
async def insert_hist_pax_revenue_rms(start_date: str, end_date: str): 
    db = SessionLocal()

    end_year = end_date.split('-')[0]
    end_month = end_date.split('-')[1]

    end_date_query = db.query(HistPaxRevenueRms.departure_date).filter(
        and_(
            extract('year', HistPaxRevenueRms.departure_date) == end_year,
            extract('month', HistPaxRevenueRms.departure_date) == end_month,
            HistPaxRevenueRms.type_rev == 0
        )
    )

    date_result = [result[0] for result in end_date_query.all()]
    start_date_c = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_date_c = datetime.strptime(end_date, '%Y-%m-%d').date()

    if start_date_c in date_result and end_date_c in date_result:
        print("Data from range "+start_date+" - "+end_date+" is found")
        return {
            'status': {
                "responseCode": status.HTTP_200_OK,
                "responseDesc": "Data already exist",
                "responseMessage": "Data cargo & pax from "+start_date+" to "+end_date+" already exist"
            }
        }
    else:
        print("Data from range "+start_date+" - "+end_date+" not found in database")
        pax = await revenue_pax_daily(None, None, start_date, end_date)
        cargo = await revenue_cargo_daily("", "", start_date, end_date)
        print("Continue Insert Data")
        if pax['status']['responseCode']==500 & cargo['status']['responseCode']==500:
            return {
                'status': {
                    "responseCode": status.HTTP_500_INTERNAL_SERVER_ERROR,
                    "responseDesc": "Internal Server Error",
                    "responseMessage": "Failed to insert cargo & pax "+start_date+"-"+end_date+"."
                }
            }
        elif pax['status']['responseCode']==404 & cargo['status']['responseCode']==404:
            return {
                'status': {
                    "responseCode": status.HTTP_404_NOT_FOUND,
                    "responseDesc": "Data not found",
                    "responseMessage": "Data cargo & pax "+start_date+"-"+end_date+" Not Found"
                }
            }
        else:
            if pax['status']['responseCode']==404:
                print("Pax data is empty, Continue insert data cargo")
                # data_queue = [item['tanggal'] for item in cargo['data']]
                # date_list = [date.split()[0] for date in data_queue]
                await insert_data_to_hist_revenue_rms(cargo)
                        
            elif cargo['status']['responseCode']==404:
                print("Cargo data is empty, Continue insert data pax")
                await insert_data_to_hist_revenue_rms(pax)
            else:
                print("Inserting data pax and cargo")
                await insert_data_to_hist_revenue_rms(cargo)
                await insert_data_to_hist_revenue_rms(pax) 

            return {
                'status': {
                    "responseCode": status.HTTP_200_OK,
                    "responseDesc": "Success",
                    "responseMessage": "Succes, data pax & cargo from "+start_date+" to "+end_date+" is inserted"
                }
            }
        