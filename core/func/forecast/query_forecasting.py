from sqlalchemy import text

schema_name = 'rms_pelni'

query_type_rev_0 = text(f"""SELECT
    hist_pax_revenue.departure_date,
    master_origin_port.code AS origin_port,
    master_destination_port.code AS destination_port,
    hist_pax_revenue.type_rev,
    hist_pax_revenue.total,
    hist_pax_revenue.revenue,
    hist_pax_revenue.type,
    hist_pax_revenue.uom
    FROM
        {schema_name}.hist_pax_revenue
    JOIN
        {schema_name}.master_port AS master_origin_port
    ON
        hist_pax_revenue.origin_port = master_origin_port.id_port
    JOIN
        {schema_name}.master_port AS master_destination_port
    ON
        hist_pax_revenue.destination_port = master_destination_port.id_port
    WHERE 
        hist_pax_revenue.type_rev = 0;""")

query_type_rev_1 = text(f"""SELECT
    hist_pax_revenue.departure_date,
    master_origin_port.code AS origin_port,
    master_destination_port.code AS destination_port,
    hist_pax_revenue.type_rev,
    hist_pax_revenue.total,
    hist_pax_revenue.revenue,
    hist_pax_revenue.type,
    hist_pax_revenue.uom
    FROM
        {schema_name}.hist_pax_revenue
    JOIN
        {schema_name}.master_port AS master_origin_port
    ON
        hist_pax_revenue.origin_port = master_origin_port.id_port
    JOIN
        {schema_name}.master_port AS master_destination_port
    ON
        hist_pax_revenue.destination_port = master_destination_port.id_port
    WHERE 
        hist_pax_revenue.type_rev = 1;""")

query_port =   text(f"""SELECT id_port, code, port_name
    FROM {schema_name}.master_port
    WHERE is_deleted = false AND flag_ppss = 'Y'
    GROUP BY id_port, code, port_name;""")


  