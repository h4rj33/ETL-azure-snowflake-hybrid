import requests
from datetime import datetime
from .config import SLACK_TOKEN, SLACK_CHANNEL

def send_slack_message(msg):
    url="https://slack.com/api/chat.postMessage"
    headers={"Authorization":f"Bearer {SLACK_TOKEN}","Content-Type":"application/json"}
    requests.post(url,headers=headers,json={"channel":SLACK_CHANNEL,"text":msg})


def slack_etl_summary(results:dict):
    header = f"*ETL Validation Report — {datetime.now():%d %b %Y %I:%M %p}*\n"
    divider="TABLE".ljust(25)+"| SRC ROWS | FINAL ROWS | STATUS\n"+"-"*60+"\n"

    rows=""
    for tbl,(src,final) in results.items():
        status="✅ MATCH" if src==final else "❌ MISMATCH"
        rows+=f"{tbl.ljust(25)} | {str(src).rjust(8)} | {str(final).rjust(10)} | {status}\n"

    send_slack_message(header+"```\n"+divider+rows+"-"*60+"\n```")
