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
    #-------Loading raw transaction pandas validated data ----------------------------
    logger.info("📥 Reading raw transaction data from S3/cleaned/temp folder...")
    unfiltered_data = spark.read.option("header","true")\
                      .option("inferschema","true")\
                      .csv("s3://financial-data-pipeline-project/data/cleaned_raw/temp/raw_transaction_data.csv")
    unfiltered_data = unfiltered_data.withColumn(
        "Date",to_date("Date")
    )
    unfiltered_data = unfiltered_data.withColumn("Day",day("Date"))\
                                     .withColumn("Month",month("Date"))\
                                     .withColumn("Year",year("Date"))
    unfiltered_data.write.partitionBy("Year","Month","Day")\
                   .mode("append")\
                   .parquet("s3://financial-data-pipeline-project/data/cleaned_raw/partitioned/")

    # ---------- Load raw transactions ---------- ---- ----- ----
    logger.info("📥 Reading raw transaction data from S3/cleaned/partitioned folder...")
    txn_df = spark.read.parquet("s3://financial-data-pipeline-project/data/cleaned_raw/partitioned/")
    logger.info("✅ Raw transaction data read successfully.")

    today = date.today()
    year = today.year
    month = today.month
    day = today.day

    txn_df= txn_df.filter((txn_df["Year"] = year) & (txn_df["Month"] = month)& (txn_df["Day"] = day))

    # Show the DataFrame before filter
    logger.info("Showing txn_df dataframe after load..")
    txn_df.show()

except Exception as e:
    import traceback
    logger.error("[ERROR] Pipeline failed:\n" + traceback.format_exc())    