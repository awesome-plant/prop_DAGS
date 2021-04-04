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

def insertData(ps_user, ps_pass, ps_host, ps_port, ps_db, table, df_insert):
    update_status=False
    rc=0
    while update_status==False:
        try:
            engine = create_engine('postgresql://' + ps_user + ':' + ps_pass + '@' + ps_host + ':' + ps_port + '/' + ps_db)
            df_insert.to_sql(
                name=table
                ,schema='sc_land'
                ,con=engine
                ,method=psql_insert_copy
                ,if_exists='append'
                ,index=False
                )
            update_status=True
        except Exception as e: 
            update_status=False
            rc+=1
            print("retry:", str(rc), '-error:', str(e))   
        
def saveProxies(ps_user, ps_pass, ps_host, ps_port, ps_db, update, df_proxy_list):
    # df_proxies, "postgres", "root", "172.22.114.65", "5432", "scrape_db"
    #this section does 2 things 
    # 1. remove list of existing proxies for said host - move raw to hist
    # 2. insert new proxies into table
    print('move from raw to his')
    print("importing roughly")
    df_proxy_list['status']='scraped'
    print('total proxies to add:',str(len(df_proxy_list)))

    try:
        #does the following things 
        # 1. move raw -> his 
        # 2. truncate raw 
        # 3. import df to raw 
        #connection details here 
        
        with psycopg2.connect(user=ps_user,password=ps_pass,host=ps_host,port=ps_port,database=ps_db,connect_timeout=30) as conn:
            with conn.cursor() as cur:
                cur.execute("""insert into sc_land.SC_PROXY_HIS 
                                (proxy, website, scrape_dt) 
                                SELECT proxy, website, scrape_dt 
                                FROM sc_land.sc_proxy_raw 
                                except 
                                SELECT proxy, website, scrape_dt 
                                FROM sc_land.sc_proxy_his
                            """)
                conn.commit()
                cur.execute("delete from sc_land.sc_proxy_raw where website= %(website)s",
                        {
                            'website': df_proxy_list['website'].drop_duplicates().to_string(index=False).strip()
                        })
                conn.commit()
                # https://stackoverflow.com/questions/18390574/how-to-delete-duplicate-rows-in-sql-server
                # delete duplicates 
                cur.execute("""delete from sc_land.sc_proxy_raw a 
                            	using sc_land.sc_proxy_raw b 
                            	where a.table_id > b.table_id 
                            	and a.proxy = b.proxy   
                            """)
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

def cleanProxies(ps_user, ps_pass, ps_host, ps_port, ps_db):
    print("final cleanup at the end")
    update_status=False
    rc=0
    while update_status==False:
        try:
            with psycopg2.connect(user=ps_user,password=ps_pass,host=ps_host,port=ps_port,database=ps_db,connect_timeout=30) as conn:
                with conn.cursor() as cur:
                    cur.execute("select count(*) from sc_land.sc_proxy_raw")
                    result = cur.fetchone()
                old_count=result[0]
                print("original count:", old_count)

                with conn.cursor() as cur:
                    cur.execute("""delete from sc_land.sc_proxy_raw a 
                                    using sc_land.sc_proxy_raw b 
                                    where a.table_id > b.table_id 
                                    and a.proxy = b.proxy """)
                    conn.commit()
                with conn.cursor() as cur:
                    cur.execute("select count(*) from sc_land.sc_proxy_raw")
                    result = cur.fetchone()
                new_count=result[0]
                print("new count:", new_count)
                print('proxies removed:', str(int(old_count) - int(new_count)) )
            update_status=True 
        except Exception as e: 
            update_status=False
            rc+=1
            print("retry:", str(rc), '-error:', str(e))   

def getFileID():
    connection = psycopg2.connect(user="postgres",password="root",host="172.22.114.65",port="5432",database="scrape_db",connect_timeout=30)
    cursor = connection.cursor()
    cursor.execute("SELECT coalesce(max(H_FILEID), 0) + 1 as h_fileid from sc_land.SC_SOURCE_HEADER")
    h_fileid = cursor.fetchone() #next iteration of file ID 
    return h_fileid[0]

def getDBProxy(ps_user, ps_pass, ps_host, ps_port, ps_db, update, **kwargs): 
    #gets single proxy and prox_type, updates as 'used' to stop duplication
    proxy=proxy_type=''
    with psycopg2.connect(user=ps_user,password=ps_pass,host=ps_host,port=ps_port,database=ps_db) as conn:
        with conn.cursor() as cur:
            cur.execute("select proxy, proxy_type from sc_land.sc_proxy_raw where status ='ready' order by table_id limit 1")
            result = cur.fetchone()
            if update=='used' or update=='sitemap':
                cur.execute("update sc_land.sc_proxy_raw set status = %(status)s where proxy = %(proxy)s",
                    {
                        'proxy': result[0]
                        ,'status': update
                    }
                )
                conn.commit()
    print("proxy used is:", result)
    if result is not None: 
        proxy=result[0]
        proxy_type=result[1]
    return proxy, proxy_type 

def newProxyList(df_proxies, ps_user, ps_pass, ps_host, ps_port, ps_db, **kwargs):
    result=False
    try:
        df_proxies['status']='ready'
        #does the following things 
        # 1. move raw -> his 
        # 2. truncate raw 
        # 3. import df to raw 
        #connection details here 
        
        with psycopg2.connect(user=ps_user,password=ps_pass,host=ps_host,port=ps_port,database=ps_db,connect_timeout=30) as conn:
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
            ,method=psql_insert_copy
            ,if_exists='append'
            ,index=False
            )
        result=True
    except Exception as e:
        print("error on reflight impProxy:", e)
    return result

def getProxies(ps_user, ps_pass, ps_host, ps_port, ps_db, sql_start, sql_size):
    import pandas as pd
    #queries db and returns list of all proxies within paramaters 
    with psycopg2.connect(user=ps_user,password=ps_pass,host=ps_host,port=ps_port,database=ps_db,connect_timeout=30) as conn:
        check_proxy_list=pd.read_sql_query("SELECT proxy, proxy_type FROM sc_land.sc_proxy_raw order by table_id limit " + str(sql_size) + " offset " + str(sql_start), conn)
        print("got row list, start:", str(sql_start), 'length:', str(sql_size))
    return check_proxy_list 

def getCurrentIP(ps_user, ps_pass, ps_host, ps_port, ps_db):
    with psycopg2.connect(user=ps_user,password=ps_pass,host=ps_host,port=ps_port,database=ps_db,connect_timeout=30) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT cur_ip FROM sc_land.sc_cur_ip")
                result = cur.fetchone() #next iteration of file ID 
    return result[0]

def updateProxies(ps_user, ps_pass, ps_host, ps_port, ps_db, proxy_list, value):
    #updates proxy as broken.
    update_status=False
    rc=0
    while update_status==False:
        try:
            with psycopg2.connect(user=ps_user,password=ps_pass,host=ps_host,port=ps_port,database=ps_db,connect_timeout=30) as conn:
                with conn.cursor() as cur:
                    proxy_list.apply(lambda x: 
                        cur.execute("""
                            update sc_land.sc_proxy_raw 
                            set status = %(value)s 
                            ,error = %(error)s 
                            ,req_time =%(req_time)s
                            where proxy = %(proxy)s"""
                            , { 'proxy': x['proxy'], 'value': value, 'error': x['error'], 'req_time': x['req_time'] }
                            ), axis=1 
                    )
                    conn.commit()
            update_status=True
        except Exception as e: 
            update_status=False
            rc+=1
            print("retry:", str(rc), '-error:', str(e))

def getChildPages(ps_user, ps_pass, ps_host, ps_port, ps_db, sql_start, sql_size):
    #same as get proxies
    import pandas as pd
    print("final cleanup at the end")
    update_status=False
    rc=0
    while update_status==False:
        try:
            #queries db and returns list of all proxies within paramaters 
            with psycopg2.connect(user=ps_user,password=ps_pass,host=ps_host,port=ps_port,database=ps_db,connect_timeout=30) as conn:
                child_page_list=pd.read_sql_query("select s_filename, s_fileid from sc_land.v_source_file order by table_id limit " + str(sql_size) + " offset " + str(sql_start), conn)
                print("select s_filename, s_fileid from sc_land.v_source_file order by table_id limit " + str(sql_size) + " offset " + str(sql_start))
            update_status=True
            return child_page_list
        except Exception as e: 
            update_status=False
            rc+=1
            print("retry:", str(rc), '-error:', str(e))   

def getScrapePages(ps_user, ps_pass, ps_host, ps_port, ps_db, sql_start, sql_size):
    #same as get proxies
    import pandas as pd
    #queries db and returns list of all proxies within paramaters 
    with psycopg2.connect(user=ps_user,password=ps_pass,host=ps_host,port=ps_port,database=ps_db,connect_timeout=30) as conn:
        child_page_list=pd.read_sql_query("SELECT url, prop_id FROM sc_land.sc_property_links order by table_id limit " + str(sql_size) + " offset " + str(sql_start), conn)
        print("got row list, start:", str(sql_start), 'length:', str(sql_size))
    return child_page_list

def getChildPagesCount(ps_user, ps_pass, ps_host, ps_port, ps_db, **kwargs):
    #queries and returns total number of rows in db 
    try: 
        with psycopg2.connect(user=ps_user,password=ps_pass,host=ps_host,port=ps_port,database=ps_db) as conn:
            with conn.cursor() as cur:
                cur.execute("select count(*) from sc_land.v_source_file")
                result = cur.fetchone()
        print("child pages count:", result[0])
        proxy_count=result[0]
    except Exception as e: 
        print("error on get next child pages:", e)
    return proxy_count

def getPropScrape(ps_user, ps_pass, ps_host, ps_port, ps_db, sql_start, sql_size):
    import pandas as pd
    #queries db and returns list of all proxies within paramaters 
    with psycopg2.connect(user=ps_user,password=ps_pass,host=ps_host,port=ps_port,database=ps_db,connect_timeout=30) as conn:
        check_proxy_list=pd.read_sql_query("SELECT URL, STATE, SUBURB, PROP_ID, filetype FROM sc_land.sc_prop_scrape order by table_id limit " + str(sql_size) + " offset " + str(sql_start), conn)
        print("got row list, start:", str(sql_start), 'length:', str(sql_size))
    return check_proxy_list 

def cleanupSiteMapChild(ps_user, ps_pass, ps_host, ps_port, ps_db):
    print("final cleanup at the end")
    update_status=False
    rc=0
    while update_status==False:
        try:
            with psycopg2.connect(user=ps_user,password=ps_pass,host=ps_host,port=ps_port,database=ps_db,connect_timeout=30) as conn:
                with conn.cursor() as cur:
                    cur.execute("select count(*) from sc_land.sc_property_links")
                    result = cur.fetchone()
                old_count=result[0]
                print("original count:", old_count)

                with conn.cursor() as cur:
                    cur.execute("""delete from sc_land.sc_property_links a 
                                    using sc_land.sc_property_links b 
                                    where a.table_id > b.table_id 
                                    and a.url = b.url """)
                    conn.commit()
                with conn.cursor() as cur:
                    cur.execute("select count(*) from sc_land.sc_property_links")
                    result = cur.fetchone()
                new_count=result[0]
                print("new count:", new_count)
                print('child pages removed:', str(int(old_count) - int(new_count)) )
            update_status=True
        except Exception as e: 
            update_status=False
            rc+=1
            print("retry:", str(rc), '-error:', str(e))   