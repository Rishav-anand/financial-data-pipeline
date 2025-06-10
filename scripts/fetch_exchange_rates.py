import requests
import json
import boto3
from datetime import datetime, timedelta,date
import sys

# # Load config
# with open('config.json') as f:
#     config = json.load(f)

s3 = boto3.client('s3')
bucket_name = "financial-data-pipeline-project"
app_id = "0528878812ed4c8e9ce89de7c3d88836"
base_currency = "USD"

# start_date = datetime.strptime(config["start_date"], "%Y-%m-%d")
# end_date = datetime.strptime(config["end_date"], "%Y-%m-%d")

today = date.today()

start_date = today
end_date = today

date = start_date
while date <= end_date:
    date_str = date.strftime("%Y-%m-%d")
    url = f"https://openexchangerates.org/api/historical/{date_str}.json?app_id={app_id}&base={base_currency}"
    
    try:
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"❌ Failed to fetch exchange rate for {date_str}, status code: {response.status_code}")
        filename = f"data/exchange_rates/{date_str}.json"
        try:
            s3.put_object(Bucket=bucket_name, Key=filename, Body=response.content)
            print(f"✅ Uploaded {filename} to S3.")
        except Exception as s3_err:
            raise Exception(f"❌ Failed to upload {filename} to S3: {str(s3_err)}")
    except Exception as err:
        print(str(err))
        sys.exit(1) 
    date += timedelta(days=1)
