import os
from dotenv import load_dotenv
load_dotenv()

SLACK_TOKEN = os.getenv("SLACK_TOKEN")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL")

LOG_TABLE = os.getenv("LOG_TABLE", "LOG_DB.LOG_SCHEMA.INGESTION_LOGS")

AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
AZURE_CONTAINER = os.getenv("AZURE_CONTAINER")
AZURE_FILES = []  # same default

SF_SOURCE = {
    "user": os.getenv("SF_SOURCE_USER"),
    "account": os.getenv("SF_SOURCE_ACCOUNT"),
    "private_key_file": os.getenv("SF_SOURCE_PRIVATE_KEY"),
    "warehouse": os.getenv("SF_SOURCE_WAREHOUSE"),
    "database": os.getenv("SF_SOURCE_DATABASE"),
    "schema": os.getenv("SF_SOURCE_SCHEMA"),
    "role": os.getenv("SF_SOURCE_ROLE"),
}

SF_TARGET = {
    "user": os.getenv("SF_TARGET_USER"),
    "account": os.getenv("SF_TARGET_ACCOUNT"),
    "private_key_file": os.getenv("SF_TARGET_PRIVATE_KEY"),
    "warehouse": os.getenv("SF_TARGET_WAREHOUSE"),
    "database": os.getenv("SF_TARGET_DATABASE"),
    "schema": os.getenv("SF_TARGET_SCHEMA"),
    "role": os.getenv("SF_TARGET_ROLE"),
}
