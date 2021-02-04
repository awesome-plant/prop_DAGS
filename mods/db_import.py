import psycopg2
from sqlalchemy import create_engine
from psycopg2 import Error
import csv
from io import StringIO
# https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html#io-sql-method
def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)
        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name
        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


def saveProxies(ps_user, ps_pass, ps_host, ps_port, ps_db, update, df_proxy_list):
    # df_proxies, "postgres", "root", "172.22.114.65", "5432", "scrape_db"
    #this section does 2 things 
    # 1. remove list of existing proxies for said host - move raw to hist
    # 2. insert new proxies into table
    print('move from raw to his')
    print("importing roughly")
    df_proxy_list['status']='ready'
    print(df_proxy_list.head())

    try:
        #does the following things 
        # 1. move raw -> his 
        # 2. truncate raw 
        # 3. import df to raw 
        #connection details here 
        
        with psycopg2.connect(user=ps_user,password=ps_pass,host=ps_host,port=ps_port,database=ps_db) as conn:
            with conn.cursor() as cur:
                cur.execute("insert into sc_land.SC_PROXY_HIS (proxy, website, scrape_dt) SELECT proxy, website, scrape_dt FROM sc_land.sc_proxy_raw except SELECT proxy, website, scrape_dt FROM sc_land.sc_proxy_his;")
                conn.commit()
                cur.execute("delete from sc_land.sc_proxy_raw where website= %(website)s",
                        {
                            'website': df_proxy_list['website'].drop_duplicates().to_string(index=False).strip()
                        })
                conn.commit()
        engine = create_engine('postgresql://' + ps_user + ':' + ps_pass + '@' + ps_host + ':' + ps_port + '/' + ps_db)
        df_proxy_list.to_sql(
            name='sc_proxy_raw'
            ,schema='sc_land'
            ,con=engine
            ,method=psql_insert_copy
            ,if_exists='append'
            ,index=False
            )
        # result=True
        print("finished importing new proxies from:", df_proxy_list['website'].drop_duplicates().to_string(index=False).strip())
    except Exception as e:
        print("error on reflight impProxy:", e)


def newProxyList(df_proxies, ps_user, ps_pass, ps_host, ps_port, ps_db, **kwargs):
    result=False
    try:
        df_proxies['status']='ready'
        #does the following things 
        # 1. move raw -> his 
        # 2. truncate raw 
        # 3. import df to raw 
        #connection details here 
        
        with psycopg2.connect(user=ps_user,password=ps_pass,host=ps_host,port=ps_port,database=ps_db) as conn:
            with conn.cursor() as cur:
                cur.execute("insert into sc_land.SC_PROXY_HIS (proxy, scrape_dt, status) select proxy, now(), status from sc_land.sc_proxy_raw")
                conn.commit()
                cur.execute("truncate table sc_land.sc_proxy_raw")
                conn.commit()
        engine = create_engine('postgresql://' + ps_user + ':' + ps_pass + '@' + ps_host + ':' + ps_port + '/' + ps_db)
        df_proxies[['proxy','status']].to_sql(
            name='sc_proxy_raw'
            ,schema='sc_land'
            ,con=engine
            ,method=db_import.psql_insert_copy
            ,if_exists='append'
            ,index=False
            )
        result=True
    except Exception as e:
        print("error on reflight impProxy:", e)
    return result