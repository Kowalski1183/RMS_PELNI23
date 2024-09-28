import time
import numpy as np
import pandas as pd
import pandas.io.sql as sqlio
import warnings
import sys
import uuid

import psycopg2
import io
import csv
import os
import tempfile

from datetime import datetime, timedelta
from sqlalchemy import text
import holidays

from sklearn import metrics
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score, mean_squared_log_error
from sklearn.preprocessing import LabelEncoder

if not sys.warnoptions:
    warnings.simplefilter("ignore")
np.random.seed(42)

def generate_random_uuid():
    return str(uuid.uuid4())

def feature_encoding(df): 
    label_encoder = LabelEncoder()

    df['kode_jenis'] = label_encoder.fit_transform(df['jenis_muatan'])
    #check hasil label encode
    
    total_unique_jenis_muatan = df['jenis_muatan'].nunique()

    total_unique_kode_jenis_muatan = df['kode_jenis'].nunique()
    df['kode_route'] = df['kode_org'].astype(str) + df['kode_des'].astype(str)
    df['kode_route'] = df['kode_route'].astype(int)
    return df

def create_features_1(df):
    df['month'] = df['date'].dt.month
    df['year'] = df['date'].dt.year
    df['day'] = df['date'].dt.day
    df['dayofweek'] = df['date'].dt.dayofweek
    return df

def remove_duplicate(df):
    df=df.loc[~df.duplicated(subset=['jumlah','kode_route','kode_jenis','date'])]
    
    grouped_df = df.groupby(["date","kode_org","kode_des","kode_route","kode_jenis","year","month","day","weekofyear"])\
    .agg({"jumlah": ["sum"]})
    grouped_df.columns = ["jumlah"]
    grouped_df= grouped_df.reset_index()
    return grouped_df

def create_features_2(df,df_holidays):
    """
    Create time series features based on time series index.
    """
    df = df.copy()
    df['Is_wknd'] = df['date'].dt.dayofweek // 4 # Fri-Sun are 4-6, Monday is 0 so this is valid
    
    df['dayofweek'] = df['date'].dt.dayofweek
    df['month'] = df['date'].dt.month
    df['year'] = df['date'].dt.year
    df['dayofyear'] = df['date'].dt.dayofyear
    df['day'] = df['date'].dt.day   
    df['weekofyear'] = df['date'].dt.isocalendar().week
    df['weekofyear'] = df['weekofyear'].astype('int')
    df['is_holiday']=df['date'].apply(lambda x: 1 if x in df_holidays['day'].values else 0)
    df['is_covid'] = ((df['date'] >= '2020-03-01') & (df['date'] <= '2021-12-31')).astype(int)
    return df

def add_lags(ts):
    ts.set_index('date', inplace=True)
    target_map = ts['jumlah'].to_dict()
    ts['lag1'] = (ts.index - pd.Timedelta('7 days')).map(target_map)
    ts['lag2'] = (ts.index - pd.Timedelta('14 days')).map(target_map)
    ts['lag3'] = (ts.index - pd.Timedelta('28 days')).map(target_map)
    ts.reset_index(inplace=True)
    return ts


def custom_objective(y_true, y_pred):
    grad = (y_pred - y_true) / (y_pred + 1)  # You can adjust the denominator for your specific needs
    hess = 1.0 / (y_pred + 1)**2
    return grad, hess
  
def create_holiday(df):
    ## dataframe hari libur
    id_holidays = holidays.ID(years=range(min(df.date.dt.year), max(df.date.dt.year)+1))

    data = []

    for day, event in id_holidays.items():
        data.append({'day': day, 'event': event})
        
    df_holidays = pd.DataFrame(data)
    df_holidays['day'] = pd.to_datetime(df_holidays['day'], format='%Y/%m/%d')

    #cuti idul fitri
    index = df_holidays.loc[df_holidays['event']=="Hari Raya Idul Fitri"].index
    original_date = df_holidays.loc[index, 'day']
    for date in original_date.values:
        date = pd.to_datetime(date)
    #     print(date)
        for i in range(1, 4):
    #         print(i)
            date_before = date - timedelta(days=i)
    #         print(date_before)
            new_row = {'event': 'Cuti Hari Raya Idul Fitri', 'day': date_before}
    #         print(new_row)
            df_holidays = pd.concat([df_holidays, pd.DataFrame([new_row])], ignore_index=True)

    index = df_holidays.loc[df_holidays['event']=="Hari kedua dari Hari Raya Idul Fitri"].index
    original_date = df_holidays.loc[index, 'day']
    for date in original_date.values:
        date = pd.to_datetime(date)
    #     print(date)
        for i in range(1, 3):
    #         print(i)
            date_after = date + timedelta(days=i)
    #         print(date_before)
            new_row = {'event': 'Cuti Hari Raya Idul Fitri', 'day': date_after}
    #         print(new_row)
            df_holidays = pd.concat([df_holidays, pd.DataFrame([new_row])])
    return df_holidays

def remove_outliers_mad(df, column):
    threshold = 3.5
    median = df[column].median()
    mad = np.median(np.abs(df[column] - median))
    threshold_value = threshold * mad

    upper_bound = median+threshold_value
    lower_bound = (median - threshold_value) 

    outliers = df[(abs(df[column]) > upper_bound) | (abs(df[column]) < lower_bound)]
    # print("jumlah :",len(outliers))
    filtered_df = df.drop(outliers.index)

    return filtered_df

def bulk_insert(data,df):
    import psycopg2.pool
        # Create a connection pool with the specified parameters
    connection_pool = psycopg2.pool.ThreadedConnectionPool(
        10,  # pool_size
        10 + 50,  # pool_size + max_overflow
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_SERVER"),
        port=os.getenv("POSTGRES_PORT"),
    )
    # Define the total number of rows to insert
    totaldata = len(data)
    
    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, newline='') as csvfile:
        csv_file_path = csvfile.name
        
        # Define column names
        column_names = [
            "id_hist_pax", "is_deleted", "created_by", "created_date", "departure", "departure_date",
            "destination_port","origin_port",
            "type_rev", "revenue", "total", "type", "uom","org_code", "des_code"
        ]

        csv_writer = csv.writer(csvfile)
        
        # Write the header row with column names
        csv_writer.writerow(column_names)
        iterasi=0
        # Write the data rows
        for row in data.itertuples(index=False):
            
            # print(row)
            iterasi+=1
            id_hist_pax = generate_random_uuid()
            total = (row.pred)
            # year = int(row.year)
            # month = int(row.month)
            # day = int(row.day)
            departure_date = row.date
            
            if row.kode_des is None:
                kode_des = 'NULL'
            else:
                kode_des = int(row.kode_des)
            
            if row.kode_org is None:
                kode_org = 'NULL'
            else:
                kode_org = int(row.kode_org)

            # print(kode_org,'-',kode_des)
            # des_query = db.query(MasterPort.id_port).filter(MasterPort.code.like(f"%{kode_des}%"))
            # org_query = db.query(MasterPort.id_port).filter(MasterPort.code.like(f"%{kode_org}%"))
            
            des_result = row.destination_port
            org_result = row.origin_port
            # print(des_query)
            # print(org_query)
            des_list = []
            des_list = des_list.append(des_result)
            org_list = []
            org_list = org_list.append(org_result)
            # print(des_result)
            # print(org_result)
            # if des_result is None:
            #     des = 'NULL'
            #     # print(str(des_query))
            # else:
            #     # print(str(des_query))
            #     des = des_result[0]
            
            # if org_result is None:
            #     org = 'NULL'
            #     # print(str(org_query))
            # else:
            #     # print(str(org_query))
            #     org = org_result[0]
            
            created_date = datetime.today()
            revenue = 0
            cargo_type = (df[df.kode_jenis == row.kode_jenis]['jenis_muatan'].head(1).values[0]).upper()
                
                
            try:
                uom = (df[df.kode_jenis == row.kode_jenis]['uom'].head(1).values[0]).upper() if uom in df.columns else None
            except NameError:
                uom = None
            if (cargo_type != 'GENERAL_CARGO') or (cargo_type != 'REDPACK') :
                total = int(total)
            # Write the row to the CSV file
            csv_writer.writerow([id_hist_pax, False, 'Forcaster', created_date, departure_date, departure_date,
                                des_result, org_result, 2, revenue, total, cargo_type, uom,kode_org, kode_des ])
            if iterasi % 100 == 0:
                progress = (iterasi/totaldata)*100

                print(f"Progress :  {progress:.2f}%")
    print("finish mapping data")
    # Establish a database connection
    # print(des_list)
    # print(org_list)
    connection = connection_pool.getconn()

    cursor = connection.cursor()

    csv_buffer = io.StringIO()
    csv_buffer.write(open(csv_file_path, 'r').read())

    csv_buffer.seek(0)

    # Use the COPY command to bulk insert data from the StringIO buffer
    copy_sql = f"""
        COPY rms_pelni.hist_pax_revenue (
            {', '.join(column_names)}
        ) FROM STDIN WITH CSV HEADER;
    """
    cursor.copy_expert(copy_sql, csv_buffer)

    connection.commit()

    cursor.close()
    connection.close()
    connection_pool.closeall()
    os.remove(csv_file_path)
    print("Temporary CSV file deleted.")