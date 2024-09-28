from fastapi import APIRouter, status
from typing import Optional

from core.endpoint import FORECASTHISTPAX
from core.schemas.schemas import ForecastHistPax
from core.database.sessions import SessionLocal  
from core.func.forecast import functions,query_forecasting
from sklearn.model_selection import train_test_split
from core.database.sessions import engine, settings
import time

import xgboost as xgb
import multiprocessing
import itertools

from sklearn.metrics import r2_score
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psycopg2.pool

from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score, mean_squared_log_error
from sklearn.preprocessing import LabelEncoder

import holidays

from hyperopt import fmin, tpe, hp, STATUS_OK, Trials
from hyperopt.pyll.base import scope
from sqlalchemy import text

router = APIRouter()

@router.post(FORECASTHISTPAX, tags=["Forecast Service"])
async def forecast_hist_pax(request:ForecastHistPax):
    start = time.time()

    db = SessionLocal()
    period_start = request.period_start
    period_end = request.period_end
    
    df_type_rev_0 = pd.read_sql(query_forecasting.query_type_rev_0, engine)
    df_type_rev_1 = pd.read_sql(query_forecasting.query_type_rev_1, engine)
    # print(df_type_rev_0)
    # print(df_type_rev_1)
    type_rev_0_set = set()
    selected_data = []
    for _, row in df_type_rev_0.iterrows():
        od_pair = (row['origin_port'], row['destination_port'])
        type_rev_0_set.add(od_pair)
        selected_data.append({'date':row['departure_date'],'kode_org': row['origin_port'], 'kode_des': row['destination_port'],
                            'type_rev': row['type_rev'], 'jumlah': row['total'], 'revenue': row['revenue'],'jenis_muatan':row['type'],'uom':row['uom']})


    # Step 4: ITERASI TYPE_REV 1, CEK OD pairs that are not already in type_rev 0 set
    for _, row in df_type_rev_1.iterrows():
        od_pair = (row['origin_port'], row['destination_port'])
        if od_pair not in type_rev_0_set:
            # If the OD pair is not available in the data with type_rev 0, then add it to the selected_data list
            selected_data.append({'date':row['departure_date'],'origin_port': row['origin_port'], 'destination_port': row['destination_port'],
                                'type_rev': row['type_rev'], 'total': row['total'], 'revenue': row['revenue'],'type':row['type'],'uom':row['uom']})
    final_df = pd.DataFrame(selected_data)  
    final_df['date'] = pd.to_datetime(final_df['date'], format='%Y/%m/%d')
    
    print(final_df)

    
    import datetime

    # Get the current date and time
    current_date_time = datetime.datetime.now()

    # Extract the year from the current date and time
    this_year = current_date_time.year
    start_of_year = datetime.datetime(current_date_time.year, 12, 1)

    # Get the end date of the current year
    end_of_year = datetime.datetime(current_date_time.year, 12, 31)
    
    
    print(period_start,'-',period_end)
    if period_start.strip() != "" and period_end.strip() != "":
        try:
            start_of_year = datetime.datetime.strptime(period_start, "%Y-%m-%d")
            start_year = start_of_year.year
            end_of_year = datetime.datetime.strptime(period_end, "%Y-%m-%d")
            end_year = end_of_year.year
        except ValueError:
            # Handle the case where the input dates are not in the expected format
            # You can log an error message or return an appropriate response
            # For example, return a 400 Bad Request response indicating invalid date format
            return {
                "status": {
                    "responseCode": status.HTTP_400_BAD_REQUEST,
                    "responseDesc": "Bad Request",
                    "responseMessage": "Invalid date format. Use YYYY-MM-DD format."
                }
            }

    else:
        # Handle the case where period_start or period_end is empty
        # You can log an error message or return an appropriate response
        # For example, return a 400 Bad Request response indicating missing dates
            return {
                "status": {
                    "responseCode": status.HTTP_400_BAD_REQUEST,
                    "responseDesc": "Bad Request",
                    "responseMessage": "Missing start or end date."
                }
            }

    print(start_of_year,'-',end_of_year)
    
    first_day_of_first_week = (start_of_year - datetime.timedelta(days=start_of_year.weekday())).strftime('%Y-%m-%d')

    # Calculate the last day of the last week of the year
    last_day_of_last_week = (end_of_year + datetime.timedelta(days=(6 - end_of_year.weekday()))).strftime('%Y-%m-%d')

    main_df = functions.create_features_1(final_df)
    min_year = min(main_df.year)
    if min_year < this_year:
        df = main_df[(main_df.year > (start_year-7))&(main_df.year <= start_year-1)]
    else:
        df = main_df


       # Replacement dictionary
    replacement_dict = {
        'REEFER CONTAINER': 'REEFER_CONTAINER',
        'DRY CONTAINER': 'DRY_CONTAINER',
        'GENERAL CARGO': 'GENERAL_CARGO'
    }

    # Replace values in the 'type' column
    df['jenis_muatan'] = df['jenis_muatan'].replace(replacement_dict)
    df = df[df.jenis_muatan!='CONTAINER']
    df = df.dropna(subset=['jenis_muatan', 'kode_org', 'kode_des'])
    # df.dropna(subset=['jenis_muatan'])
    print(df.jenis_muatan.unique())
    # df.dropna(subset=['kode_org'])
    print(df.kode_org.unique())
    # df.dropna(subset=['kode_des'])
    print(df.kode_des.unique())
    df['kode_org'] = df['kode_org'].astype(int)
    df['kode_des'] =df['kode_des'].astype(int)
    
    
    print('Dataframe W/O nan combination',df)

    
    numeric_col = ['total','revenue']
    str_col = ['jenis_muatan','uom','kode_route','date']
    target_label = 'jumlah'
    df = functions.feature_encoding(df)
    # print(df)
    
    ## dataframe hari libur
    df_holidays = functions.create_holiday(df)
            
    # print(df_holidays)
            
    grouped_df = functions.create_features_2(df,df_holidays)
    grouped_df = functions.remove_duplicate(grouped_df)
    grouped_df = functions.add_lags(grouped_df)
    # print(grouped_df.shape)
    
    # result_port = db.execute(query_port )
    # port_data = [dict(row_port) for row_port in result_port]
    # df_port = pd.DataFrame(port_data)
    df_port = pd.read_sql_query(query_forecasting.query_port,engine)
    
    print(df_port)
    df_port['code'] = df_port['code'].astype(int)
    nan_id_port_rows = df_port[df_port['id_port'].isna()]
    print(nan_id_port_rows)
    print(df_port.dtypes)

    from itertools import product
    
    #permutation based on name
    permutation = list(product(df_port['port_name'], repeat=2))

    #permutation based on code
    permutation_code = list(product(df_port['code'], repeat=2))

    # Create a new DataFrame for origin and destination combinations
    permutation_df = pd.DataFrame(permutation, columns=['Origin', 'Destination'])

    # Create a new DataFrame for origin and destination combinationsvenv\Scripts\activate
    permutation_df_code = pd.DataFrame(permutation_code, columns=['Origin', 'Destination'])
    permutation_df = permutation_df[permutation_df['Origin'] != permutation_df['Destination']]
    permutation_df_code = permutation_df_code[permutation_df_code['Origin'] != permutation_df_code['Destination']]
    permutation_df_code.rename(columns={'Origin': 'kode_org', 'Destination': 'kode_des'}, inplace=True)
    # print(permutation_df_code)
    
    
    distinct_kode_jenis = grouped_df['kode_jenis'].unique()

    # Create a new DataFrame to store the mutated future data
    future = pd.DataFrame(columns=['kode_org','kode_des','kode_jenis'])

    # Iterate through each distinct kode_jenis and create new rows in mutated_future
    for kode_jenis in distinct_kode_jenis:
        print(kode_jenis)
    #     uom_value = df[df['kode_jenis'] == kode_jenis]['uom'].iloc[0]
        kode_uom_value = df[df['kode_jenis'] == kode_jenis]['kode_uom'].iloc[0]
        new_row = permutation_df_code.copy()
        new_row['kode_jenis']=kode_jenis
        new_row['kode_uom']=kode_uom_value
    #     new_row['uom']= uom_value
        future = pd.concat([future, new_row], ignore_index=True)
        # print("THIS IS FUTURE",future)
        
        
    dfs = []
    
    # Sample values for org, des, and cluster


    # Loop through the combinations of org, des, and cluster
    x = 1
    for index, row in future.iterrows():
        kode_org = row['kode_org']
        kode_des = row['kode_des']
        kode_jenis = row['kode_jenis']
        kode_uom = row['kode_uom']
        date_range = pd.date_range(start=start_of_year, end=end_of_year, freq='D')
        
        # Create a new DataFrame with org, des, cluster, and date columns
        new_df = pd.DataFrame({'kode_org': kode_org,
                                'kode_des': kode_des,
                                'kode_jenis': kode_jenis,
                                'kode_uom': kode_uom,
                                'date': date_range})
        
        # Append the new_df to the list of DataFrames
        dfs.append(new_df)
        
        if x % 10000 == 0: 
            print(f'iterasi ke - {x}')
        x+=1
        
    future_date = pd.concat(dfs, ignore_index=True)
    print("finished create permutation OD + TYPE + date ")
    future_date['type_rev']=2
    future_date = functions.create_features_2(future_date,df_holidays)   
    print("finished  extract date ")
    grouped_df['type_rev']=1
    ts_and_future = pd.concat([grouped_df, future_date])
    ts_and_future = functions.add_lags(ts_and_future)
    print("finished  add lags")
    future_w_features= ts_and_future.loc[ts_and_future.type_rev==2].copy()
    future_w_features['kode_org'] = future_w_features['kode_org'].astype(int)
    future_w_features['kode_des'] = future_w_features['kode_des'].astype(int) 
    
    output_cargo = pd.DataFrame()
    result = []
    
    def hyperparameter_tuning(space):
        model = xgb.XGBRegressor(**space)
        
        #Define evaluation datasets.
        evaluation = [(X_train, y_train), (X_test, y_test)]
        
        #Fit the model. Define evaluation sets, early_stopping_rounds, and eval_metric.
        model.fit(X_train, y_train,
                eval_set=evaluation, eval_metric="rmse",
                early_stopping_rounds=100,verbose=False)

        #Obtain prediction and rmse score.
        pred = model.predict(X_test)
        rmse = mean_squared_error(y_test, pred, squared=False)
        print ("SCORE:", rmse)
    
        #Specify what the loss is for each model.
        return {'loss':rmse, 'status': STATUS_OK, 'model': model}

    space = {'max_depth': scope.int(hp.quniform("max_depth", 1, 5, 1)),
            'gamma': hp.uniform ('gamma', 0,1),
            'reg_alpha' : hp.uniform('reg_alpha', 0,50),
            'reg_lambda' : hp.uniform('reg_lambda', 10,100),
            'colsample_bytree' : hp.uniform('colsample_bytree', 0,1),
            'min_child_weight' : hp.uniform('min_child_weight', 0, 5),
            'n_estimators': 500,
            'learning_rate': hp.uniform('learning_rate', 0, .15),
            'tree_method':'auto', 
            # 'gpu_id': 0,
            'random_state': 5,
            'nthread': multiprocessing.cpu_count(),
            'max_bin' : scope.int(hp.quniform('max_bin', 200, 550, 1))}
    x=1
    for type_code in sorted(grouped_df.kode_jenis.unique()):
        print(type_code)
        ts = grouped_df.loc[grouped_df.kode_jenis==type_code]
        
        all_columns = ts.columns.tolist()
        exclude_columns = numeric_col+str_col
        feature_columns = [col for col in all_columns if col not in exclude_columns]
        datas = pd.DataFrame()
        
        X_train = pd.DataFrame()
        X_test = pd.DataFrame()
        y_train = pd.DataFrame()
        y_test = pd.DataFrame()
        for route_code in sorted(grouped_df.kode_route.unique()):
         route_ts = ts[ts.kode_route==route_code]
         route_ts = functions.remove_outliers_mad(route_ts,'jumlah')
         datas = pd.concat([datas, route_ts])
         
        X = datas[feature_columns]
        y = datas['jumlah']

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        if min_year < this_year:
        
         X_train = datas[datas.year < this_year][feature_columns]
         X_test = datas[datas.year == this_year][feature_columns]
         y_train = datas[datas.year < this_year]['jumlah']
         y_test = datas[datas.year == this_year]['jumlah']
        # print(X_train)
        
        if len(X_train) > 0:
            
            data_pred = future_w_features[future_w_features.kode_jenis==type_code][feature_columns]
            trials = Trials()
            best = fmin(fn=hyperparameter_tuning,
                        space=space,
                        algo=tpe.suggest,
                        max_evals=50,
                        trials=trials)
            print(f'Kode Jenis : {type_code}, best param : {best}')
            best_params = {
                'max_depth': int(best['max_depth']),
                'reg_lambda': int(best['reg_lambda']),
                'n_estimators': 500,
                'reg_alpha': int(best['reg_alpha']),
                'colsample_bytree': int(best['colsample_bytree']),
                'min_child_weight':int(best['min_child_weight']),
                # 'gpu_id': 1,
                'tree_method':'auto',
                'nthread': multiprocessing.cpu_count(),
                # 'random_state': 0
            }


            xgb_model = xgb.XGBRegressor(objective=functions.custom_objective,**best_params)
            xgb_model.fit(X,y)
            y_pred = xgb_model.predict(data_pred)
            data_pred['pred'] = np.maximum(y_pred, 0)
            output_cargo = pd.concat([output_cargo, data_pred])
            # if x > 0:
            #     break
            # x+=1
    
    output_cargo = output_cargo[['kode_org','kode_des','kode_jenis','kode_uom','year','month','day','weekofyear','type_rev','pred']]
    print(output_cargo.dtypes)
    # cur = con.cursor()
    query_lweek_output = text(f"""select departure_date as date, org_code as kode_org, des_code  as kode_des, "type"  as jenis_muatan, uom , total  as pred,type_rev
    FROM rms_pelni.hist_pax_revenue
    WHERE type_rev = 2 and departure_date >= '{first_day_of_first_week}' and departure_date <= '{last_day_of_last_week}';""")
    
    df_lweek_output = pd.read_sql(query_lweek_output, engine)
    print('THIS IS DF LWEEK OUTPUT',df_lweek_output)
    
    if len(df_lweek_output) >0:
        df_lweek_output['date'] = pd.to_datetime(df_lweek_output['date'], format='%Y/%m/%d')
        df_lweek_output = functions.feature_encoding(df_lweek_output )
        df_lweek_output = functions.create_features_2(df_lweek_output,df_holidays)
        df_lweek_output = df_lweek_output[['kode_org','kode_des','kode_jenis','kode_uom','year','month','day','weekofyear','type_rev','pred']]
    print(query_lweek_output)
    print(df_lweek_output)
    final_output =  pd.concat([output_cargo, df_lweek_output])
    print(final_output.dtypes)
    label_encoder = LabelEncoder()
    final_output['route'] = final_output['kode_org'].astype(str) + '-' + final_output['kode_des'].astype(str)
    final_output['route_encoded'] = label_encoder.fit_transform(final_output['route'])
    final_output = final_output.rename(columns={'route_encoded': 'kode_route'})
    
    max_pred_index =final_output.groupby(['kode_jenis', 'kode_route', 'weekofyear'])['pred'].idxmax()

    # Select the rows with the highest 'pred' using the index
    result_df = final_output.loc[max_pred_index]
    subset_columns = ['kode_route', 'kode_jenis','weekofyear']

    # Remove duplicate rows based on the specified subset of columns
    result_cleaned = result_df.drop_duplicates(subset=subset_columns)
    result_cleaned['kode_org'] = result_cleaned['kode_org'].astype(int)
    result_cleaned['kode_des'] = result_cleaned['kode_des'].astype(int)
    
    insert_data = (
    result_cleaned
    .merge(df_port[['id_port', 'code']], left_on='kode_org', right_on='code', how='left')
    .rename(columns={'id_port': 'origin_port'})
    .drop('code', axis=1)
    .merge(df_port[['id_port', 'code']], left_on='kode_des', right_on='code', how='left')
    .rename(columns={'id_port': 'destination_port'})
    .drop('code', axis=1)
    )
    print("Finish Retrieve Origin and Destination Port from df_port")
    # print(result_cleaned)
    # merged_result = result_cleaned.merge(df_port[['id_port', 'code']], 
    #                                 left_on='kode_org', 
    #                                 right_on='code', 
    #                                 how='left').drop('code', axis=1)

    # # Rename the 'id_port' column to 'origin_port'
    # merged_result.rename(columns={'id_port': 'origin_port'}, inplace=True)
    # insert_data = merged_result.merge(df_port[['id_port', 'code']], 
    #                                 left_on='kode_des', 
    #                                 right_on='code', 
    #                                 how='left').drop('code', axis=1)
    # insert_data.rename(columns={'id_port': 'destination_port'}, inplace=True)
    insert_data['pred'] = insert_data['pred'].astype(int)
    print(insert_data)
    # nan_ports = insert_data[insert_data['origin_port'].isna() | insert_data['destination_port'].isna()]

    # print(nan_ports)
    # print(df_port[df_port.id_port=='fkt3qi1w7afgjnpktg64ui231h8'])
    # print(insert_data[insert_data.origin_port=='fkt3qi1w7afgjnpktg64ui231h8'])
    # print(insert_data[insert_data.destination_port=='fkt3qi1w7afgjnpktg64ui231h8'])
    forecastend = time.time()
    fore = (forecastend-start)/60
    print('forcasting success : '+str(fore))

    # cur = con.cursor()
    query_delete =  text(f"""DELETE FROM rms_pelni.hist_pax_revenue WHERE type_rev = 2 and departure_date >= '{first_day_of_first_week}' and departure_date <= '{last_day_of_last_week}';""")
  
    db.execute(query_delete)
    #     #commit the transcation 
    db.commit()
    # x = 1
    deleteend = time.time()
    dele = (deleteend-start)/60
    print("data is deleted : "+str(dele))
    # error =[]
    #Bulk Insert
    # # for week in result_cleaned['weekofyear'].unique():
    # #     data = result_cleaned[result_cleaned.weekofyear==week]
    # # try:
    functions.bulk_insert(insert_data ,df)
    # except Exception as error:
    #     Handle the case where the input dates are not in the expected format
    #     You can log an error message or return an appropriate response
    #     For example, return a 400 Bad Request response indicating invalid date format
    #     return {
    #         "status": {
    #             "responseCode": status.HTTP_400_BAD_REQUEST,
    #             "responseDesc": "Insert Failed",
    #             "responseMessage": error
    #         }
    #     }

    endwrite = time.time()
    print("write succsess : "+ str((endwrite-start)/60))

    end = time.time()
    return {
        "status": {
            "responseCode": status.HTTP_200_OK,
            "responseDesc": "Success",
            "responseMessage": "Success Updating Forecast Data"
            ,"period_start":start_of_year
            ,"period_end":end_of_year
            ,"time": (end-start)/60
            # ,"lenData":len(result_cleaned)
            # ,"errrorWeek":error
        }
        # ,"result": res
    }       