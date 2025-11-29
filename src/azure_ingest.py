import io,pandas as pd,traceback
from datetime import datetime,UTC
from azure.storage.blob import BlobServiceClient
from snowflake.connector.pandas_tools import write_pandas
from .config import AZURE_CONNECTION_STRING,AZURE_CONTAINER,AZURE_FILES,SF_TARGET
from .connections import get_sf_connection
from .log_utils import log_ingestion
from .slack_utils import send_slack_message

def run_azure_ingestion():
    results={}
    service=BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)

    if AZURE_FILES:
        files=AZURE_FILES
    else:
        container=service.get_container_client(AZURE_CONTAINER)
        files=[b.name for b in container.list_blobs() if b.name.lower().endswith(".csv")]  

    for file in files:
        try:
            src,final=ingest_from_azure(file)
            results[file.upper()]=(src,final)
        except:
            results[file.upper()]=(0,0)

    return results


def ingest_from_azure(blob_name):
    try:
        service=BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        conn=get_sf_connection(SF_TARGET)
        cur=conn.cursor()

        blob=service.get_blob_client(AZURE_CONTAINER,blob_name)
        df=pd.read_csv(io.BytesIO(blob.download_blob().readall()))

        if df.columns[0]=="Prop_0":   
            df.columns=df.iloc[0];df=df[1:]

        df["ETL_INSERT_DATE"]=datetime.now(UTC).strftime("%Y-%m-%d")

        base=blob_name.split("/")[-1].split(".")[0].upper()
        l1,f2=f"{base}_TEMP",base

        success,nchunks,nrows,_=write_pandas(conn,df,table_name=l1,overwrite=True,auto_create_table=True,use_logical_type=True)
        log_ingestion("AZURE","L1",l1,nrows)

        if nrows>0:
            cols=[f'"{c}"' for c in df.columns if c.upper()!="ETL_INSERT_DATE"]
            cur.execute(f"""
                CREATE OR REPLACE TABLE {f2} AS
                SELECT {",".join(cols)},CURRENT_TIMESTAMP() ETL_INSERT_DATE
                FROM {l1}
            """)
            final=cur.execute(f"SELECT COUNT(*) FROM {f2}").fetchone()[0]
        else:
            final=0

        log_ingestion("AZURE","L2",f2,final)
        return nrows,final

    except Exception as e:
        traceback.print_exc()
        send_slack_message(f"ETL Failure {blob_name}\n{e}")
        return 0,0
