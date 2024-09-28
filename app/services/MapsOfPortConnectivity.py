from fastapi import APIRouter
from fastapi.responses import HTMLResponse
from fastapi import HTTPException
from scgraph.geographs.marnet import marnet_geograph

import folium
import random
import pandas as pd
import searoute as sr
from core.schemas.schemas import *
from geopy.distance import geodesic
from core.endpoint import MAPS
from core.func.maps.functions import conn_db_maps
from core.models.modelsrms import MasterShipRms
from core.database.sessions import SessionLocal


router = APIRouter()

# @router.post(MAPS, response_class=HTMLResponse, tags=["Maps Service"])
async def get_map(request_data_list: List[ListMapsRequest]):
    db = SessionLocal()

    map_obj = folium.Map(location=[-2.438, 119.004], zoom_start=5)

    colors = ['red', 'blue', 'green', 'purple', 'orange', 'darkred',
                'darkblue', 'darkgreen', 'cadetblue', 'darkpurple', 'pink', 'gray', 'black']
    
    color_maker = ['red', 'blue', 'green', 'purple', 'orange', 'darkred', 'lightred',
                    'darkblue', 'darkgreen', 'cadetblue', 'darkpurple', 'pink','lightgreen', 'gray', 'black', 'lightgray']
    
    if not request_data_list or all(not request_data.idPort for request_data in request_data_list):
        return map_obj._repr_html_()
   
    for request_data in request_data_list :
        ship_query = db.query(MasterShipRms.name).filter(MasterShipRms.id_ship.like(f"%{request_data.idShip}"))
        ship_result = ship_query.first()
        
        id_port_list = request_data.idPort
        df = await conn_db_maps(id_port_list)
        if df.empty:
            raise HTTPException(status_code=404, detail="Data not found")

        # Convert the 'latitude' and 'longitude' columns to numeric type
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

        # Filter out rows with invalid latitude and longitude values
        invalid_latitudes = df[(df['latitude'] > 90) | (df['latitude'] < -90)]
        invalid_longitudes = df[(df['longitude'] > 180) | (df['longitude'] < -180)]

        invalid_rows = pd.concat([invalid_latitudes, invalid_longitudes])
        if not invalid_rows.empty:
            print("Invalid latitude or longitude values detected:")
            print(invalid_rows)
            df = df.drop(invalid_rows.index)
            if df.empty:
                raise HTTPException(status_code=404, detail="All latitude and longitude values are invalid.")
        
        # Create a folium map centered at the location of the first point
        initial_latitude = df.iloc[0]['latitude']
        initial_longitude = df.iloc[0]['longitude']
        
        list_color = []
        # Add markers for each point on the map
        for i, row in df.iterrows():
            port_latitude = row['latitude']
            port_longitude = row['longitude']
            port_name = row['port_name']
            # Calculate the distance between the point and the initial location
            distance = geodesic((initial_latitude, initial_longitude), (port_latitude, port_longitude)).kilometers
            # marker's popup
            popup_text = f"Port Name: {port_name}<br>Ship Name: {ship_result[0]}<br>Distance: {distance:.2f} km"
            color = random.choice(color_maker)
            list_color.append(color)
            folium.Marker(
                location=[port_latitude, port_longitude],
                popup=folium.Popup(popup_text, max_width=200, max_height=200),
                tooltip=port_name,
                icon=folium.Icon(icon="ship", color=color, prefix="fa")
            ).add_to(map_obj)

        # create polylines to connect the points and show the distance on the popups
        polylines = []
        for i in range(len(df) - 1):
            coordinates = marnet_geograph.get_shortest_path(
                origin_node = {"latitude":df.iloc[i]['latitude'], "longitude":df.iloc[i]['longitude']},
                destination_node = {"latitude":df.iloc[i + 1]['latitude'], "longitude":df.iloc[i + 1]['longitude']}
            )
            coordinates = [[i['latitude'],i['longitude']] for i in coordinates['coordinate_path']]

            for coord in coordinates:
                folium.CircleMarker(location=coord, radius=1 ,fill_color='black',color='black').add_to(map_obj)
                
            start = (df.iloc[i]['latitude'], df.iloc[i]['longitude'])
            end = (df.iloc[i]['latitude'], df.iloc[i]['longitude'])
            distance = geodesic(start,end).kilometers
            popup_name_ship = f"Ship Name: {ship_result}"
            popup_name_port = f"Port Name: {port_name}"
            popup_distance = f"Distance: {distance:.2f} km"
            popup_content = f"{popup_name_port}\n{popup_name_ship}\n{popup_text}"
            polyline = folium.PolyLine(locations=coordinates, popup_text=popup_content, color=list_color[0])

            polylines.append(polyline)
            polyline.add_to(map_obj)

    return map_obj._repr_html_()