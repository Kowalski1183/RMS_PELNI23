# pelnirms-python-be
PELNI Route Management System - Python Backend.

TODO: put Developer Guide (process flow, how to run, configuration guides) here.

## Services :
 ### 1. Retrieve Service
   #### - Revenue Cargo Yearly
      > describe...
   #### - Revenue Cargo Daily
      > describe...
   #### - Revenue Pax Yearly
      > describe...
### 2. Calculator Service
   #### - Cost of Route
      > describe...
   ####  - Est Revenue Factor
      > describe...
   #### - Est Cost Revenue Factor
      > describe...
### 3. Maps Service
   #### - Get Map
### 4. Insert Service
   #### - Insert Hist Pax Revenue Stg
      > describe...
   #### - Insert Hist Pax Revenue Rms
      > describe...
### 5. Forecast Service
   #### - Forecase Hist Pax
      > describe...
### 6. Route Optimizer Service
   #### - Route Optimizer
      > describe...

## Requirement 
   ```
   Package                   Version
   ------------------------- ------------
   annotated-types           0.5.0
   anyio                     3.7.1
   asttokens                 2.4.0
   attrs                     23.1.0
   backcall                  0.2.0
   branca                    0.6.0
   certifi                   2023.7.22
   charset-normalizer        3.2.0
   click                     8.1.7
   cloudpickle               2.2.1
   colorama                  0.4.6
   comm                      0.1.4
   contourpy                 1.1.0
   cycler                    0.11.0
   debugpy                   1.6.7.post1
   decorator                 5.1.1
   et-xmlfile                1.1.0
   executing                 1.2.0
   fastapi                   0.103.1
   fastjsonschema            2.18.0
   folium                    0.14.0
   fonttools                 4.42.1
   future                    0.18.3
   geographiclib             2.0
   geopy                     2.4.0
   greenlet                  2.0.2
   h11                       0.14.0
   hijri-converter           2.3.1
   holidays                  0.32
   httpcore                  0.17.3
   httpx                     0.24.1
   hyperopt                  0.2.7
   idna                      3.4
   import-ipynb              0.1.4
   ipykernel                 6.25.2
   ipython                   8.15.0
   jedi                      0.19.0
   Jinja2                    3.1.2
   joblib                    1.3.2
   jsonschema                4.19.0
   jsonschema-specifications 2023.7.1
   jupyter_client            8.3.1
   jupyter_core              5.3.1
   kiwisolver                1.4.5
   MarkupSafe                2.1.3
   matplotlib                3.8.0
   matplotlib-inline         0.1.6
   nbformat                  5.9.2
   nest-asyncio              1.5.7
   networkx                  3.1
   newrelic                  9.0.0
   numpy                     1.25.2
   openpyxl                  3.1.2
   packaging                 23.1
   pandas                    2.1.0
   parso                     0.8.3
   pickleshare               0.7.5
   Pillow                    10.0.0
   pip                       23.2.1
   platformdirs              3.10.0
   prompt-toolkit            3.0.39
   psutil                    5.9.5
   psycopg2                  2.9.7
   pure-eval                 0.2.2
   py4j                      0.10.9.7
   pydantic                  2.3.0
   pydantic_core             2.6.3
   Pygments                  2.16.1
   pyparsing                 3.1.1
   python-dateutil           2.8.2
   python-dotenv             1.0.0
   pytz                      2023.3.post1
   pywin32                   306
   pyzmq                     25.1.1
   referencing               0.30.2
   requests                  2.31.0
   rpds-py                   0.10.2
   scikit-learn              1.3.0
   scipy                     1.11.2
   seaborn                   0.12.2
   setuptools                68.2.2
   six                       1.16.0
   sklearn                   0.0.post9
   sniffio                   1.3.0
   SQLAlchemy                2.0.20
   stack-data                0.6.2
   starlette                 0.27.0
   threadpoolctl             3.2.0
   tornado                   6.3.3
   tqdm                      4.66.1
   traitlets                 5.9.0
   typing                    3.7.4.3
   typing_extensions         4.7.1
   tzdata                    2023.3
   urllib3                   2.0.4
   uuid                      1.30
   uvicorn                   0.23.2
   wcwidth                   0.2.6
   xgboost                   2.0.0
   ```

## How To Run :
### Command : 
   ```
   python main.py

   // running on backgroud using nohup
   nohup python main.py 2> pyoutput.log 
   ```
### Running with New Relic 
   ```
   // Validate newrelic.ini
   newrelic-admin validate-config newrelic.ini

   // Running
   NEW_RELIC_CONFIG_FILE=newrelic.ini newrelic-admin run-program python main.py
   ```
