import httpx
from typing import List
import uuid
from datetime import datetime
from core.models.models import HistPaxRevenue, MasterPort, MasterShip
from core.models.modelsrms import HistPaxRevenueRms,MasterPortRms,MasterShipRms
from core.database.sessions import SessionLocal

async def fetch_data_from_url(url, headers=None, params=None):
    try:
        async with httpx.AsyncClient(headers=headers) as client:
            response = await client.get(url=url, params=params)
            if response.status_code >= 500:
                # Handle internal server error
                error_message = f"Internal Server Error: {response.status_code} - {response.text}"
                return {"error": error_message}
            response.raise_for_status()
            data = response.json()  # Attempt to decode the JSON response
            return data
    except (httpx.RequestError, httpx.DecodingError) as e:
        # Handle other errors gracefully and return an error message
        error_message = f"Error fetching data from {url}: {e}"
        print(error_message)
        return {"error": error_message}


async def insert_data_to_hist_revenue_stg(combined_data: List[dict]):
    db = SessionLocal()
    try:
        for item in combined_data['data']:

            des_query = db.query(MasterPort.id_port).filter(MasterPort.code.like(f"%{item['kode_des']}%"))
            org_query = db.query(MasterPort.id_port).filter(MasterPort.code.like(f"%{item['kode_org']}%"))
            des_result = des_query.first()
            org_result = org_query.first()

            ship_query = db.query(MasterShip.id_ship).filter(MasterShip.ship_code.like(f"%{item['kode_kapal']}"))
            ship_result = ship_query.first()

            type=''
            uom=''
            revenue=''
            total=''
            if 'penghasilan_muatan' in item:
                type=item['jenis_muatan']
                uom=item['uom']
                revenue=item['penghasilan_muatan']
                total=item['jumlah']
            else:
                type="PASSANGER"
                uom='PERSON'
                revenue=item['penghasilan_penumpang']
                total=item['pax']

            departure_date = None
            try:
                departure_date = datetime.strptime(item["tanggal"], '%Y-%m-%d')
            except:
                departure_date = datetime.strptime(item["tanggal"], '%Y-%m-%d %H:%M:%S')
            
            departure_time = None
            try:
                departure_time = datetime.strptime(item["tanggal"], '%Y-%m-%d %H:%M:%S').time()
            except ValueError:
                pass 

            if des_result is None or org_result is None:
                code = item['kode_org'] or item['kode_des']
                port_name = item['org'] or item['des']

                db_master = MasterPort(
                    id_port = uuid.uuid4(),
                    code = code,
                    port_name = port_name,
                    is_deleted = False,
                    flag_data_source = 'PELNI',
                    active_status = True,
                    fuel = False,
                    created_date = datetime.now()
                )
                db.add(db_master)
                db.commit()

                des_query = db.query(MasterPort.id_port).filter(MasterPort.code.like(f"%{item['kode_des']}%"))
                org_query = db.query(MasterPort.id_port).filter(MasterPort.code.like(f"%{item['kode_org']}%"))
                des_result = des_query.first()
                org_result = org_query.first()

                db_item = HistPaxRevenue(
                    id_hist_pax=uuid.uuid4(),
                    # deleted_by=None,
                    # deleted_date=None,
                    is_deleted=False,
                    created_by="System",
                    created_date=datetime.now(),
                    # changed_by=None,
                    # changed_date=None,
                    departure=departure_date,
                    departure_date=departure_date,
                    departure_time=departure_time,
                    # voyage=None,
                    destination_port=des_result[0],
                    origin_port=org_result[0],
                    idship=ship_result[0],
                    stop=item['jumlah_transit'],
                    ship_name=item['kapal'],
                    ship_code=item['kode_kapal'],
                    type=type.upper(),
                    uom=uom.upper(),
                    total=total,
                    revenue=revenue,
                    org_name=item['org'],
                    org_code=item['kode_org'],
                    des_name=item['des'],
                    des_code=item['kode_des'],
                    type_rev=0,
                 )
                db.add(db_item)
                db.commit()


            else :
                db_item = HistPaxRevenue(
                   id_hist_pax=uuid.uuid4(),
                    # deleted_by=None,
                    # deleted_date=None,
                    is_deleted=False,
                    created_by="System",
                    created_date=datetime.now(),
                    # changed_by=None,
                    # changed_date=None,
                    departure=departure_date,
                    departure_date=departure_date.date(),
                    departure_time=departure_time,
                    # voyage=None,
                    destination_port=des_result[0],
                    origin_port=org_result[0],
                    idship=ship_result[0],
                    stop=item['jumlah_transit'],
                    ship_name=item['kapal'],
                    ship_code=item['kode_kapal'],
                    type=type.upper(),
                    uom=uom.upper(),
                    total=total,
                    revenue=revenue,
                    org_name=item['org'],
                    org_code=item['kode_org'],
                    des_name=item['des'],
                    des_code=item['kode_des'],
                    type_rev=0,
                 )
                db.add(db_item)
                db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

async def insert_data_to_hist_revenue_rms(combined_data: List[dict]):   

    db = SessionLocal()
    try:
        for item in combined_data['data']:

            des_query = db.query(MasterPortRms.id_port).filter(MasterPortRms.code.like(f"%{item['kode_des']}%"))
            org_query = db.query(MasterPortRms.id_port).filter(MasterPortRms.code.like(f"%{item['kode_org']}%"))
            des_result = des_query.first()
            org_result = org_query.first()

            ship_query = db.query(MasterShipRms.id_ship).filter(MasterShipRms.ship_code.like(f"%{item['kode_kapal']}"))
            ship_result = ship_query.first()

            type=''
            uom=''
            revenue=''
            total=''
            if 'penghasilan_muatan' in item:
                type=item['jenis_muatan']
                uom=item['uom']
                revenue=item['penghasilan_muatan']
                total=item['jumlah']
            else:
                type="PASSANGER"
                uom='PERSON'
                revenue=item['penghasilan_penumpang']
                total=item['pax']

            departure_date = None
            try:
                departure_date = datetime.strptime(item["tanggal"], '%Y-%m-%d')
            except:
                departure_date = datetime.strptime(item["tanggal"], '%Y-%m-%d %H:%M:%S')
            
            departure_time = None
            try:
                departure_time = datetime.strptime(item["tanggal"], '%Y-%m-%d %H:%M:%S').time()
            except ValueError:
                pass 

            if des_result is None or org_result is None:
                # print("port not found")
                code = item['kode_org'] or item['kode_des']
                port_name = item['org'] or item['des']

                db_master = MasterPortRms(
                    id_port = uuid.uuid4(),
                    code = code,
                    port_name = port_name,
                    is_deleted = False,
                    flag_data_source = 'PELNI',
                    active_status = True,
                    fuel = False,
                    created_date = datetime.now()
                )
                db.add(db_master)
                db.commit()

                des_query = db.query(MasterPortRms.id_port).filter(MasterPortRms.code.like(f"%{item['kode_des']}%"))
                org_query = db.query(MasterPortRms.id_port).filter(MasterPortRms.code.like(f"%{item['kode_org']}%"))
                des_result = des_query.first()
                org_result = org_query.first()

                db_item = HistPaxRevenueRms(
                    id_hist_pax=uuid.uuid4(),
                    # deleted_by=None,
                    # deleted_date=None,
                    is_deleted=False,
                    created_by="System",
                    created_date=datetime.now(),
                    # changed_by=None,
                    # changed_date=None,
                    departure=departure_date,
                    departure_date=departure_date,
                    departure_time=departure_time,
                    # voyage=None,
                    destination_port=des_result[0],
                    origin_port=org_result[0],
                    idship=ship_result[0],
                    stop=item['jumlah_transit'],
                    ship_name=item['kapal'],
                    ship_code=item['kode_kapal'],
                    type=type.upper(),
                    uom=uom.upper(),
                    total=total,
                    revenue=revenue,
                    org_name=item['org'],
                    org_code=item['kode_org'],
                    des_name=item['des'],
                    des_code=item['kode_des'],
                    type_rev=0,
                 )
                db.add(db_item)
                db.commit()

            else :
                # print("port found")
                db_item = HistPaxRevenueRms(
                   id_hist_pax=uuid.uuid4(),
                    # deleted_by=None,
                    # deleted_date=None,
                    is_deleted=False,
                    created_by="System",
                    created_date=datetime.now(),
                    # changed_by=None,
                    # changed_date=None,
                    departure=departure_date,
                    departure_date=departure_date.date(),
                    departure_time=departure_time,
                    # voyage=None,
                    destination_port=des_result[0],
                    origin_port=org_result[0],
                    idship=ship_result[0],
                    stop=item['jumlah_transit'],
                    ship_name=item['kapal'],
                    ship_code=item['kode_kapal'],
                    type=type.upper(),
                    uom=uom.upper(),
                    total=total,
                    revenue=revenue,
                    org_name=item['org'],
                    org_code=item['kode_org'],
                    des_name=item['des'],
                    des_code=item['kode_des'], # change
                    type_rev=0,
                 )
                db.add(db_item)
                db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()