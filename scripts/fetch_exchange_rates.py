import requests
import json
import boto3
from datetime import datetime, timedelta

# Load config
with open('config/config.json') as f:
    config = json.load(f)

s3 = boto3.client('s3')
bucket_name = config["s3_bucket"]
app_id = config["api_key"]
base_currency = config["base_currency"]

start_date = datetime.strptime(config["start_date"], "%Y-%m-%d")
end_date = datetime.strptime(config["end_date"], "%Y-%m-%d")

date = start_date
while date <= end_date:
    date_str = date.strftime("%Y-%m-%d")
    url = f"https://openexchangerates.org/api/historical/{date_str}.json?app_id={app_id}&base={base_currency}"
    
    response = requests.get(url)
    if response.status_code == 200:
        filename = f"exchange_rates/{date_str}.json"
        s3.put_object(Bucket=bucket_name, Key=filename, Body=response.content)
        print(f"Uploaded {filename} to S3.")
    else:
        print(f"Failed for {date_str}: {response.status_code}")
    date += timedelta(days=1)
