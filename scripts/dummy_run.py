import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import boto3
import logging
from datetime import datetime, timedelta,date

# ---------- Logger Configuration ----------
logger = logging.getLogger("py4j")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(handler)

# ---------- Spark Session ----------
spark = SparkSession.builder.appName("CurrencyNormalization").getOrCreate()
logger.info("✅ Spark session initialized")

try:
    logger.info("📥 Reading raw transaction data from S3...")
    df = spark.read.format("json")\
            .option('multiline','true')\
            .load('s3://financial-data-pipeline-project/data/raw/raw_transaction_data.csv/2025_06_19.json')
    logger.info("✅ Raw transaction data read successfully.")

    today = date.today()

    currency_list = df.select("rates").schema[0].dataType.names
    rows = df.collect()
    data=[]
    for currency in currency_list:
        value=rows['rates'][currency]
        data.append(Row(Date=today,currency=currency,rate=float(value)))
        
    df_explode = spark.createDataFrame(data)
    df_explode.show()

except Exception as e:
    import traceback
    logger.error("[ERROR] Pipeline failed:\n" + traceback.format_exc())    