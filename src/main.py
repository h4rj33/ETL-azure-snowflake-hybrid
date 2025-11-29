# Main Code
from azure_ingest import run_azure_ingestion
from snowflake_ingest import get_source_tables,run_parallel_ingestion
from slack_utils import slack_etl_summary

if __name__=="__main__":
    print("\n ETL PIPELINE STARTED\n")

    source_tables=get_source_tables()
    azure_results=run_azure_ingestion()
    snowflake_results=run_parallel_ingestion(source_tables,max_workers=6)

    final={**azure_results,**snowflake_results}
    slack_etl_summary(final)

    print("\n ETL PIPELINE FINISHED\n")
