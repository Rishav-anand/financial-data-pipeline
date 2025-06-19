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
    txn_df = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv("s3://financial-data-pipeline-project/data/raw/raw_transaction_data.csv")
    logger.info("✅ Raw transaction data read successfully.")

    today = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    txn_df = txn_df.withColumn("Date",txn_df["Date"].cast(StringType()))

    txn_df_new = txn_df.filter(txn_df['Date'] == today)

    txn_df_new.show()

except Exception as e:
    import traceback
    logger.error("[ERROR] Pipeline failed:\n" + traceback.format_exc())    