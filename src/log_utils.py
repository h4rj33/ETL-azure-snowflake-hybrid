from .connections import get_sf_connection
from .config import SF_TARGET, LOG_TABLE
import traceback

def log_ingestion(source, layer, table, count):
    conn = get_sf_connection(SF_TARGET)
    cur  = conn.cursor()

    try:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {LOG_TABLE}(
            SOURCE STRING, DATA_LAYER STRING, TABLE_NAME STRING,
            TIMESTAMP TIMESTAMP_NTZ, RECORDS_INSERTED NUMBER
        )
        """)
        cur.execute(
            f"INSERT INTO {LOG_TABLE}(SOURCE,DATA_LAYER,TABLE_NAME,TIMESTAMP,RECORDS_INSERTED) VALUES(%s,%s,%s,CURRENT_TIMESTAMP(),%s)",
            (source,layer,table,count)
        )
        conn.commit()

    except Exception as e:
        traceback.print_exc()

    finally:
        cur.close()
        conn.close()
