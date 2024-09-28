from fastapi import APIRouter, status
from typing import Optional

from core.endpoint import FORECASTHISTPRO
from core.schemas.schemas import ForecastHistPro
from core.database.sessions import SessionLocal  
from core.func.forecast import functions,query_forecasting
from sklearn.model_selection import train_test_split,cross_val_score
from core.database.sessions import engine, settings
import time

import xgboost as xgb
import multiprocessing
import itertools
from itertools import product

from sklearn.metrics import r2_score
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psycopg2.pool

from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score, mean_squared_log_error
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import RobustScaler
import holidays

from hyperopt import fmin, tpe, hp, STATUS_OK, Trials
from hyperopt.pyll.base import scope
from sqlalchemy import text

router = APIRouter()

@router.post(FORECASTHISTPRO, tags=["Forecast HistPro Service"])
async def forecast_hist_pro(request:ForecastHistPro):
    start = time.time()
    print("Start)")
    period_start = request.period_start
    period_end = request.period_end
    
    #line 41-73 pengambilan dataset dari database
    df_type_rev_0 = pd.read_sql(query_forecasting.query_type_rev_0, engine)#rms_pelni.hist_pax_rev type_rev == 0
    df_type_rev_1 = pd.read_sql(query_forecasting.query_type_rev_1, engine)#rms_pelni.hist_pax_rev type_rev == 1
    df_port = pd.read_sql_query(query_forecasting.query_port,engine)
    
    type_rev_0_set = set()
    selected_data = []
    for _, row in df_type_rev_0.iterrows():
        
        od_pair = (row['origin_port'], row['destination_port'],row['departure_date'],row['type'])
        type_rev_0_set.add(od_pair)
        selected_data.append({'date':row['departure_date'],
                            'kode_org': row['origin_port'], 
                            'kode_des': row['destination_port'],
                            'type_rev': row['type_rev'],
                            'jumlah': row['total'], 
                            'revenue': row['revenue'],
                            'jenis_muatan':row['type']})


    for _, row in df_type_rev_1.iterrows():
        od_pair = (row['origin_port'], row['destination_port'],row['departure_date'],row['type'])
        if od_pair not in type_rev_0_set:#Cek Origin - Destination - Dep_date ada/tidak di type_rev = 0
            #kalau tidak ada masukkan kedalam list untuk dijadikan dataframe bersama dengan type_rev = 0
            selected_data.append({'date':row['departure_date'],
                                'kode_org': row['origin_port'], 
                                'kode_des': row['destination_port'],
                                'type_rev': row['type_rev'], 
                                'jumlah': row['total'], 
                                'revenue': row['revenue'],
                                'jenis_muatan':row['type']})
    final_df = pd.DataFrame(selected_data)  #ubah dari list ke dataframe
    final_df['date'] = pd.to_datetime(final_df['date'], format='%Y/%m/%d') #ubah format Dep_date menjadi datetime
    final_df = final_df.dropna(subset=['jenis_muatan', 'kode_org', 'kode_des','jumlah']) #drop data duplicate berdasarkan OD+jenis+jumlah
    #Line 76-81 rename column menyesuaikan dengan format 
    replacement_dict = {
    'REEFER CONTAINER': 'REEFER_CONTAINER',
    'DRY CONTAINER': 'DRY_CONTAINER',
    'GENERAL CARGO': 'GENERAL_CARGO',
    'Container' : 'CONTAINER'
    }
    final_df['jenis_muatan'] = final_df['jenis_muatan'].replace(replacement_dict)#apply rename 
    final_df = final_df[final_df.jenis_muatan!='CONTAINER']   #exclude container , karena kebutuhan waktu itu belum perlu container
    print("Create Historical (Done)")
    print(len(final_df))
    
    print(period_start,'-',period_end)
    #Exception handling kalau request tidak sesuai
    if period_start.strip() != "" and period_end.strip() != "":
        try:
            start_of_year = datetime.datetime.strptime(period_start, "%Y-%m-%d")#ubah string di period_start jadi datetime
            start_year = start_of_year.year #ambil year dari period_start
            end_of_year = datetime.datetime.strptime(period_end, "%Y-%m-%d")#ubah string di period_end jadi datetime
            end_year = end_of_year.year #ambil year dari period_end
        except ValueError:
            return {
                "status": {
                    "responseCode": status.HTTP_400_BAD_REQUEST,
                    "responseDesc": "Bad Request",
                    "responseMessage": "Invalid date format. Use YYYY-MM-DD format."
                }
            }
    else:
            return {
                "status": {
                    "responseCode": status.HTTP_400_BAD_REQUEST,
                    "responseDesc": "Bad Request",
                    "responseMessage": "Missing start or end date."
                }
            }

    print(start_of_year,'-',end_of_year)
    #Ambil hari pertama dan terakhir dari minggu pertama dan terakhir dari rentang periode input, ambil senin dari minggu period_start untuk start, dan minggu dari minggu period_end untuk end 
    first_day_of_first_week = (start_of_year - datetime.timedelta(days=start_of_year.weekday())).strftime('%Y-%m-%d') #misal start= 2023-11-1 first_day_of_first_week = 2023-10-30
    last_day_of_last_week = (end_of_year + datetime.timedelta(days=(6 - end_of_year.weekday()))).strftime('%Y-%m-%d') #misal end= 2023-11-1 first_day_of_first_week = 2023-10-5
    #tadinya dipakai untuk mengambil data yang sudah ada buat diseleksi bersama hasil forecast harian, untuk di seleksi ambil best jumlah di tiap minggunya
    #tapi karena ada perubahan flow jadi 2x forecast dan forecast yang dilakukan lgsg perminggu, sekarang cuma dipakai buat delete existing prediction biar ga numpuk
    print(first_day_of_first_week ,'-',last_day_of_last_week)
    end = time.time()
    
    main_df = functions.create_features_1(final_df)#ambil year buat kondisi di bawah
    min_year = min(main_df.year)
    #Kondisi kalau data historikal yang berada 1 tahun di bawah period_start ambil data tahun-tahun sebelum period_start, kalau ga ada ambil data apa adanya
    if min_year < start_year:
        #kalau ada sort lgi ambil maksimal beda 7 tahun dan minimal beda 1 tahun dari period start
        df = main_df[(main_df.year > (start_year-7))&(main_df.year <= start_year-1)][['date',
                                                                                'kode_org',
                                                                                'kode_des',
                                                                                'type_rev',
                                                                                'jumlah',
                                                                                'jenis_muatan']]
    else: 
        df = main_df[['date',
                'kode_org',
                'kode_des',
                'type_rev',
                'jumlah',
                'jenis_muatan']]
    print(df)
    print(df[df.type_rev==0])
    print(df[df.type_rev==1])
    print("Sort Historical Range (Done)")
    
    df = functions.feature_encoding(df) #labeling jenis_muatan dan create kode_route
    df_holidays = functions.create_holiday(df) #Buat dataframe hari libur dengan tambahan modifikasi libur idul fitri menjadi 7hari
    grouped_df = functions.create_features_2(df,df_holidays) #Extract data dep_date menjadi is_holiday,weekofyear,...,year
    grouped_df = functions.remove_duplicate(grouped_df) #Remove duplicate dan groupby jalur (OD+jenis+tgl) yang sama dengan sum jumlah
    grouped_df = functions.create_features_2(grouped_df,df_holidays) #Dilakukan lagi karena setelah di groupbye ada column yang hilang
    grouped_df['kode_comb'] = grouped_df.apply(lambda row: str(row['kode_org']) + str(row['kode_des']) + str(row['kode_jenis']), axis=1) #kode 1 line tanpa dep_Date (OD+jenis)
    grouped_df['type_rev']=1
    print(grouped_df)
    print("Historical Feature Prep (Done)")
    
    #line 154 - 188 generate X_prediction/Data untuk diprediksi, data yang di grouped_df itu data untuk training, kalau future_date untuk prediksi
    #Buat menentukan data yang di prediksi, itu di ambil dari kombinasi kode_port di master_port, lalu di tambahkan dengan kode_jenis, trus generate tgl harian berdasarkan periode input
    #Kalau sudah jadi di filter ambil yang OD+Jenisnya ada/terdaftar di historikal dan projection saja
    df_port['code'] = df_port['code'].astype(int)
    nan_id_port_rows = df_port[df_port['id_port'].isna()]
    permutation = list(product(df_port['port_name'], repeat=2))
    permutation_code = list(product(df_port['code'], repeat=2))
    permutation_df = pd.DataFrame(permutation, columns=['Origin', 'Destination'])
    permutation_df_code = pd.DataFrame(permutation_code, columns=['Origin', 'Destination'])
    permutation_df = permutation_df[permutation_df['Origin'] != permutation_df['Destination']]
    permutation_df_code = permutation_df_code[permutation_df_code['Origin'] != permutation_df_code['Destination']]
    permutation_df_code.rename(columns={'Origin': 'kode_org', 'Destination': 'kode_des'}, inplace=True)
    print(f'Jumlah permutasi unique port : {len(permutation_df_code)}')
    distinct_kode_jenis = grouped_df['kode_jenis'].unique()
    print("Permutation (Done)") 
    #^^Permutasi kode_port dari master_port
    future = pd.DataFrame(columns=['kode_org','kode_des','kode_jenis'])
    for kode_jenis in distinct_kode_jenis:
    
        new_row = permutation_df_code.copy()
        new_row['kode_jenis']=kode_jenis
    
        future = pd.concat([future, new_row], ignore_index=True)

    dfs = []
    #menambahkan kode_jenis dan tgl harian ke hasil permutasi kode_port
    for index, row in future.iterrows():
        kode_org = row['kode_org']
        kode_des = row['kode_des']
        kode_jenis = row['kode_jenis']
    
        date_range = pd.date_range(start=start_of_year, end=end_of_year, freq='D')
        new_df = pd.DataFrame({'kode_org': kode_org,
                                'kode_des': kode_des,
                                'kode_jenis': kode_jenis,
                                
                                'date': date_range})
        dfs.append(new_df)
        
    future_date = pd.concat(dfs, ignore_index=True) #dataframe X_prediksi
    print(future_date)
    print("Create Future (Done)")
    keys_set = set(grouped_df['kode_comb'])
    future_date = functions.create_features_2(future_date,df_holidays) 
    future_date['kode_comb'] = future_date.apply(lambda row: str(row['kode_org']) 
                                                + str(row['kode_des'])
                                                + str(row['kode_jenis']), axis=1)
    #line 201 - 218Filter untuk memisahkan data prediksi yang tidak ada di historikal dan projection
    future_date['train_flag'] = future_date['kode_comb'].apply(lambda x: 1 if x in keys_set else 0)
    future_date['type_rev']=2
    sorted_future_date = future_date[future_date.train_flag == 1][['date',
                                                                'kode_org',
                                                                'kode_des',
                                                                'kode_jenis',
                                                                'kode_comb',
                                                                'Is_wknd',
                                                                'dayofweek',
                                                                'month',
                                                                'year',
                                                                'day',
                                                                'weekofyear',
                                                                'is_holiday',
                                                                'is_covid',
                                                                'type_rev']]
    print(sorted_future_date)
    #line 220-226 itu menggabungkan data prediksi dengan train, ini bisa dipakai buat ngelihat perbandingan grafik perbandingan Train dan pred, generate lags, dll
    # untuk skrg ga ada yg dipake cuma mengikuti format sebelumnya karena sebelumnya ada generate lags
    hist_and_future = pd.concat([grouped_df, sorted_future_date])
    hist_and_future.kode_org = (hist_and_future['kode_org'].astype('int'))
    hist_and_future.kode_des = (hist_and_future.kode_des.astype('int'))
    hist_and_future.kode_comb = (hist_and_future.kode_comb.astype('int'))
    print(hist_and_future)
    
    hist = hist_and_future[hist_and_future.type_rev!=2] #pisahkan data train dari data campuran train+pred
    print(hist)
    print("Create Ts and Future per Day")
    #function objective digunakan untuk memberikan score tiap evaluasi di hyperopt supaya bisa diambil best parameter/model
    def objective(params):
        params['n_estimators'] = int(params['n_estimators'])
        model = xgb.XGBRegressor(**params)
        score = -np.mean(cross_val_score(model, X_train, y_train, cv=10, scoring='neg_mean_squared_error'))#acuan scorenya pakai MSE
        return score
    
    #forecast
    future_w_histpro = pd.DataFrame()
    for type_code in sorted(grouped_df.kode_jenis.unique()): #training model dan prediksi dilakukan sendiri-sendiri tiap jenis muatan
        ts= hist[hist.kode_jenis == type_code] #ambil data train per jenis
        all_columns = ts.columns.tolist()
        exclude_columns = ['jumlah','date','type_rev','kode_route'] #kolom yang di exclude dari variabel X


        x=1
        scaler_y = RobustScaler()
        ts.jumlah = scaler_y.fit_transform(ts.jumlah .values.reshape(-1, 1)) #transformasi nilai jumlah pada data train per jenis
        feature_columns = [col for col in all_columns if col not in exclude_columns] #kolom yang menjadi variabel X
        X = ts[feature_columns]
        y = ts['jumlah']
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        #space ini kumpulan parameter untuk di hyperopt    
        space = {'max_depth': scope.int(hp.quniform("max_depth", 1, 5, 1)),
            'gamma': hp.uniform ('gamma', 0,1),
            'reg_alpha' : hp.uniform('reg_alpha', 0,50),
            'reg_lambda' : hp.uniform('reg_lambda', 10,100),
            'colsample_bytree' : hp.uniform('colsample_bytree', 0,1),
            'min_child_weight' : hp.uniform('min_child_weight', 0, 5),
            'n_estimators': hp.quniform('n_estimators', 50, 1000, 1),
            'learning_rate': hp.uniform('learning_rate', 0, .15),
            'tree_method':'auto', 
            'random_state': 42,
            'nthread': multiprocessing.cpu_count(),
            'max_bin' : scope.int(hp.quniform('max_bin', 200, 550, 1))}

        trials = Trials()
        #best parameter diambil dari best score yaitu min value dari objective 
        best = fmin(fn=objective, space=space, algo=tpe.suggest, max_evals=30, trials=trials)#hyperopt , max eval = jumlah evaluasi kalau 30 berarti akan dilakukan training sebanyak 30x 
        
        #mapping best parameter
        best_params = {
            'n_estimators': int(best['n_estimators']),
            'max_depth': int(best['max_depth']),
            'learning_rate': best['learning_rate'],
            'colsample_bytree': best['colsample_bytree'],
            'min_child_weight': int(best['min_child_weight']),
            'gamma': best['gamma'],
            'reg_alpha': best['reg_alpha'],
            'reg_lambda': best['reg_lambda'],
            'tree_method': 'auto',  
            'random_state': 42,      
            'nthread': multiprocessing.cpu_count(),  
            'max_bin': int(best['max_bin']),  
        }

        # Train the optimized model
        optimized_model = xgb.XGBRegressor(**best_params)
        optimized_model.fit(X_train, y_train)

        # Make predictions on the test set
        y_pred_scaled = optimized_model.predict(X_test)
        y_pred_denormalized = scaler_y.inverse_transform(y_pred_scaled.reshape(-1, 1))
        # Clip predicted values to a minimum of 0
        X_test['pred'] = np.clip(y_pred_denormalized, 0, None)

        # Evaluate the model
        mse = mean_squared_error(y_test, y_pred_denormalized)
        print(f'Mean Squared Error on Test Set (Clipped): {mse}')
        
        
        #Data Prediksi
        future_f_pred = hist_and_future[(hist_and_future.type_rev==2)&(hist_and_future.kode_jenis == type_code)]
        future_f_pred = future_f_pred[feature_columns]
        # Train the optimized model
        optimized_model = xgb.XGBRegressor(**best_params)
        optimized_model.fit(X, y) #fit all ts

        # Make predictions on the test set
        # Make predictions on the test set
        y_pred_scaled = optimized_model.predict(future_f_pred)
        y_pred_denormalized = scaler_y.inverse_transform(y_pred_scaled.reshape(-1, 1))
        # Clip predicted values to a minimum of 0
        future_f_pred['pred'] = np.clip(y_pred_denormalized, 0, None) #jadi hasil forecast tiap jenis itu di kumpulkan di future_f_pred
        future_w_histpro = pd.concat([future_w_histpro, future_f_pred]) #agar hasilnya ga ketimpa dan ke append , hasilnya trus di concat ke future_w_histpro
    print("First Forecast Done")    
    print(future_w_histpro.describe())
    #check statistika descriptive hasil prediksi tiap kode jenis
    for type_code in sorted(future_w_histpro.kode_jenis.unique()):
        print(f"Prediksi")
        print(f"Kode Jenis :{type_code} " )
        print(future_w_histpro[future_w_histpro.kode_jenis==type_code].pred.describe())

    import datetime
    first_forecast = future_w_histpro[['kode_org','kode_des','kode_jenis','year','month','day','weekofyear','pred']]
    first_output = first_forecast
    first_output['kode_route'] = first_output['kode_org'].astype(str) + first_output['kode_des'].astype(str)
    first_output['kode_route'] = first_output['kode_route'].astype(int)
    
    #Pilih index yang akan diambil beradasrkan max pred per OD+jenis+week
    max_pred_index =first_output.groupby(['kode_jenis', 'kode_route', 'weekofyear'])['pred'].idxmax()
    first_result = first_output.loc[max_pred_index] 
    
    #drop duplicate , incase ada yg ga ke exclude (perminggu >1 baris)
    subset_columns = ['kode_route', 'kode_jenis','weekofyear']
    first_result = first_result.drop_duplicates(subset=subset_columns)
    first_result['kode_org'] = first_result['kode_org'].astype(int)
    first_result['kode_des'] = first_result['kode_des'].astype(int)
    
    #karena tadi di drop datenya , generate ulang pakai kolom-kolom dari functions.create_features_2, **karena tujuan forecast pertama memang ambil datenya**
    first_result['date'] = first_result.apply(lambda row: datetime.datetime(int(row['year']), int(row['month']), int(row['day'])), axis=1)
    print(first_result.date)
    print(first_result.date.unique())
    print(first_result.pred.describe())
   
    #start preparasi forecast ke-2 forecast jumlah muatan mingguan
    for type_code in sorted(first_result.kode_jenis.unique()):
        print(f"Prediksi")
        print(f"Kode Jenis :{type_code} " )
        print(first_result[first_result.kode_jenis==type_code].pred.describe())
    #Ubah data harian di grouped_df jadikan mingguan saja lalu jika 1 minggu sebelumnya ada  >1hari , jumlahnya di rata-ratakan karena pelni minta hasilnya mirip dgn rata-rata        
    weekly_hist_pax = grouped_df.groupby(['weekofyear', 'year','month','kode_org','kode_jenis', 'kode_des','kode_route'])['jumlah'].mean().reset_index()
    #Ambil hasil prediksi pertama selain date  sebagai X prediksi selanjutnya
    future_date = first_result[['weekofyear', 'year','month','kode_org', 'kode_des','kode_jenis','kode_route']]
    future_date['type_rev']=2
    weekly_hist_pax['type_rev']=1
    ts_and_future = pd.concat([weekly_hist_pax, future_date]) #gabungkan train dan prediksi
    ts_and_future['kode_org'] = ts_and_future['kode_org'].astype(int)
    ts_and_future['kode_des'] = ts_and_future['kode_des'].astype(int)
    future_w_features= ts_and_future.loc[ts_and_future.type_rev==2].copy() 
    weekly_hist = ts_and_future[ts_and_future.type_rev!=2] #Pisahkan weekly training dengan data weekly prediction
    print("Create Historikal and future [Weekly] (Done)")
    weekly_future_w_histpro = pd.DataFrame()
    #forecast weekly prediction perjenis juga
    for type_code in sorted(weekly_hist.kode_jenis.unique()):
        ts= weekly_hist[weekly_hist.kode_jenis == type_code]
        all_columns = ts.columns.tolist()
        exclude_columns = ['jumlah','date','type_rev','kode_route']


        x=1
        scaler_y = RobustScaler()
        ts.jumlah = scaler_y.fit_transform(ts.jumlah .values.reshape(-1, 1))
        feature_columns = [col for col in all_columns if col not in exclude_columns]
        X = ts[feature_columns]
        y = ts['jumlah']
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        space = {'max_depth': scope.int(hp.quniform("max_depth", 1, 5, 1)),
            'gamma': hp.uniform ('gamma', 0,1),
            'reg_alpha' : hp.uniform('reg_alpha', 0,50),
            'reg_lambda' : hp.uniform('reg_lambda', 10,100),
            'colsample_bytree' : hp.uniform('colsample_bytree', 0,1),
            'min_child_weight' : hp.uniform('min_child_weight', 0, 5),
            'n_estimators': hp.quniform('n_estimators', 50, 1000, 1),
            'learning_rate': hp.uniform('learning_rate', 0, .15),
            'tree_method':'auto', 
            'random_state': 42,
            'nthread': multiprocessing.cpu_count(),
            'max_bin' : scope.int(hp.quniform('max_bin', 200, 550, 1))}

        trials = Trials()
        best = fmin(fn=objective, space=space, algo=tpe.suggest, max_evals=30, trials=trials)

        best_params = {
            'n_estimators': int(best['n_estimators']), 
            'max_depth': int(best['max_depth']),
            'learning_rate': best['learning_rate'],
            'colsample_bytree': best['colsample_bytree'],
            'min_child_weight': int(best['min_child_weight']),
            'gamma': best['gamma'],
            'reg_alpha': best['reg_alpha'],
            'reg_lambda': best['reg_lambda'],
            'tree_method': 'auto',  # Assuming you want to keep this fixed value
            'random_state': 42,      # Assuming you want to keep this fixed value
            'nthread': multiprocessing.cpu_count(),  # Assuming you want to use the number of available CPUs
            'max_bin': int(best['max_bin']),  # Please replace 'sampled_max_bin' with the correct variable name
        }

        # Train the optimized model
        optimized_model = xgb.XGBRegressor(**best_params)
        optimized_model.fit(X_train, y_train)

        # Make predictions on the test set
        y_pred_scaled = optimized_model.predict(X_test)
        y_pred_denormalized = scaler_y.inverse_transform(y_pred_scaled.reshape(-1, 1))
        # Clip predicted values to a minimum of 0
        X_test['pred'] = np.clip(y_pred_denormalized, 0, None)

        # Evaluate the model
        mse = mean_squared_error(y_test, y_pred_denormalized)
        print(f'Mean Squared Error on Test Set (Clipped): {mse}')
        
        
        #Data Prediksi
        future_f_pred = ts_and_future[(ts_and_future.type_rev==2)&(ts_and_future.kode_jenis == type_code)]
        future_f_pred = future_f_pred[feature_columns]
        # Train the optimized model
        optimized_model = xgb.XGBRegressor(**best_params)
        optimized_model.fit(X, y) #fit all ts

        # Make predictions on the test set
        # Make predictions on the test set
        y_pred_scaled = optimized_model.predict(future_f_pred)
        y_pred_denormalized = scaler_y.inverse_transform(y_pred_scaled.reshape(-1, 1))
        # Clip predicted values to a minimum of 0
        future_f_pred['pred'] = np.clip(y_pred_denormalized, 0, None)
        weekly_future_w_histpro = pd.concat([weekly_future_w_histpro, future_f_pred])
    #Untuk proses forecast 1-2 sama beda di data train dan prediksinya saja
    print("Second Forecast Done")
    print(weekly_future_w_histpro.describe())
    for type_code in sorted(weekly_future_w_histpro.kode_jenis.unique()):
        print(f"Prediksi")
        print(df[df.kode_jenis==type_code].jenis_muatan.head(1).values[0], ' Type Code : ',type_code)
        print(weekly_future_w_histpro[weekly_future_w_histpro.kode_jenis==type_code].pred.describe())
    import datetime

    # Ambil date dari result forecast pertama
    first_result['date'] = pd.to_datetime(first_result[['year', 'month', 'day']]) #amb
    best_date = first_result[['date','kode_jenis','kode_des','kode_org','weekofyear','year']]
    best_date['kode_route'] = best_date['kode_org'].astype(str) +best_date['kode_des'].astype(str)
    best_date['kode_route'] = best_date['kode_route'].astype('int64')
    print(best_date)
    print("best date",best_date.date.unique())
    
    #buat kode rute yang sudah di drop di forecast untuk merging ambil date
    weekly_future_w_histpro['kode_route'] = weekly_future_w_histpro['kode_org'].astype(str) +weekly_future_w_histpro['kode_des'].astype(str)
    weekly_future_w_histpro['kode_route'] = weekly_future_w_histpro['kode_route'].astype('int64')
    
    #merge hasil forecast pertama dengan kedua , pertama butuh date, kedua butuh jumlah_muatan/pred nya
    weekly_pred = weekly_future_w_histpro.merge(
    best_date[['weekofyear','year','kode_jenis','kode_des','kode_org', 'kode_route', 'date']],
    on=['weekofyear','year','kode_jenis','kode_des','kode_org', 'kode_route'],
    how='left'
    )
    weekly_pred.pred.describe()
    print("weeklypred",weekly_pred.date.unique())
    
    #Ambil id port dan kode port dari master port untuk menyesuaikan format insert di bulk insert
    result_df=weekly_pred
    result_df= (
    result_df
    .merge(df_port[['id_port', 'code']], left_on='kode_org', right_on='code', how='left') #ambil id port dan kode port berdasarkan kode_org == kode_port dari master_port
    .rename(columns={'id_port': 'origin_port'})
    .drop('code', axis=1)
    .merge(df_port[['id_port', 'code']], left_on='kode_des', right_on='code', how='left') #ambil id port dan kode port berdasarkan kode_des == kode_port dari master_port
    .rename(columns={'id_port': 'destination_port'})
    .drop('code', axis=1)
    )
    print(result_df)
    forecastend = time.time()
    fore = (forecastend-start)/60
    print('forcasting success : '+str(fore))
    
    #delete existing forecast data in range period
    db = SessionLocal()
    query_delete =  text(f"""DELETE FROM rms_pelni.hist_pax_revenue WHERE type_rev = 2 and departure_date >= '{first_day_of_first_week}' and departure_date <= '{last_day_of_last_week}';""")
    print(query_delete)
    db.execute(query_delete)
    db.commit()
    deleteend = time.time()
    dele = (deleteend-start)/60
    print("data is deleted : "+str(dele))
    
    #insert Data
    functions.bulk_insert(result_df ,df)

    endwrite = time.time()
    print("write succsess : "+ str((endwrite-start)/60))

    return {
        "status": {
            "responseCode": status.HTTP_200_OK,
            "responseDesc": "Success",
            "responseMessage": "Success Updating Forecast Data"
            ,"period_start":start_of_year
            ,"period_end":end_of_year
            ,"time": (end-start)/60
            ,"lenData":len(result_df)
        }
    }       