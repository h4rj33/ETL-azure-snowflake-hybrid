import pandas as pd,traceback
from datetime import datetime,UTC
from snowflake.connector.pandas_tools import write_pandas
from concurrent.futures import ThreadPoolExecutor,as_completed
from .connections import get_sf_connection
from .log_utils import log_ingestion
from .config import SF_SOURCE,SF_TARGET

def get_source_tables():
    conn=get_sf_connection(SF_SOURCE)
    cur=conn.cursor()
    cur.execute(f"""
        SELECT TABLE_NAME FROM {SF_SOURCE['database']}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA='{SF_SOURCE['schema']}'
    """)
    return [r[0] for r in cur.fetchall()]


def ingest_from_snowflake(table):
    src=get_sf_connection(SF_SOURCE)
    tgt=get_sf_connection(SF_TARGET)
    cur=tgt.cursor()

    df=pd.read_sql_query(f"SELECT * FROM {SF_SOURCE['database']}.{SF_SOURCE['schema']}.{table}",src)
    df["ETL_INSERT_DATE"]=datetime.now(UTC).strftime("%Y-%m-%d")

    l1,l2=f"{table}_TEMP",table
    success,nchunks,nrows,_=write_pandas(tgt,df,table_name=l1,overwrite=True,auto_create_table=True)
    log_ingestion("SNOWFLAKE","L1",l1,nrows)

    if nrows>0:
        cols=[f'"{c}"' for c in df.columns if c.upper()!="ETL_INSERT_DATE"]
        cur.execute(f"""
            CREATE OR REPLACE TABLE {l2} AS
            SELECT {",".join(cols)},CURRENT_TIMESTAMP() ETL_INSERT_DATE
            FROM {l1}
        """)
        final=cur.execute(f"SELECT COUNT(*) FROM {l2}").fetchone()[0]
    else:
        final=0

    log_ingestion("SNOWFLAKE","L2",l2,final)
    return nrows,final


def run_parallel_ingestion(tables,max_workers=6):
    results={}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures={pool.submit(ingest_from_snowflake,t):t for t in tables}
        for fut in as_completed(futures):
            tbl=futures[fut]
            try:results[tbl]=fut.result()
            except:results[tbl]=(0,0)
    return results
