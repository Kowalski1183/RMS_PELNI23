from core.database.sessions import engine

import pandas as pd

async def conn_db_maps(portList):
    sql = """
        SELECT port_name, code, longitude, latitude
        FROM rms_pelni.master_port
        WHERE longitude ~ '^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$'
            AND latitude ~ '^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$'
            AND id_port = ANY(%(portList)s);
        """
    params = {'portList':portList}
    df = pd.read_sql_query(sql,engine,params=params)
    return df

async def conn_db_maps_all():
    sql = """
        SELECT port_name, code, longitude, latitude
        FROM rms_pelni.master_port
        WHERE longitude ~ '^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$'
            AND latitude ~ '^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$'
            AND id_port IN ('6c52555f-7600-41ee-8a2a-9927a743c8b5','268f0392-8e92-4f27-9a11-5c2703cf62df','f757a369-8a39-4910-a47f-68dad517d1fb');
        """
    df = pd.read_sql_query(sql,engine)
    return df
