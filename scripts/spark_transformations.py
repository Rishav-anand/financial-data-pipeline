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
    # ---------- Load raw transactions ---------- ---- ----- ----
    logger.info("📥 Reading raw transaction data from S3...")
    txn_df = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv("s3://financial-data-pipeline-project/data/raw/raw_transaction_data.csv")
    logger.info("✅ Raw transaction data read successfully.")

    # ---------- Fetch and broadcast exchange rates ------------
    logger.info("🔄 Fetching exchange rates JSON files from S3...")
    s3 = boto3.client('s3')
    bucket_name = "financial-data-pipeline-project"
    prefix = "data/exchange_rates/"

    exchange_rates = {}

    today = date.today()

    rate_files = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    for obj in rate_files.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".json"):
            date = key.split("/")[-1].replace(".json", "")
            file = s3.get_object(Bucket=bucket_name, Key=key)
            rate_json = json.loads(file['Body'].read())
            exchange_rates[date] = rate_json.get("rates", {})
    
    # Step 1: Flatten the dictionary into a list of rows
    flattened_data = []
    for date, currencies in exchange_rates.items():
        for currency, value in currencies.items():
            flattened_data.append((date, currency, float(value)))

    # Step 2: Define schema
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("rate", FloatType(), True)
    ])

    # Step 3: Create DataFrame
    rates_df = spark.createDataFrame(flattened_data, schema=schema)

     # Show the DataFrame before filter
    logger.info("Showing txn_df dataframe..")
    txn_df.show()

    #Incremental_load
    txn_df = txn_df.filter(col('Date') == today)

    # Show the DataFrame
    logger.info("Showing txn_df dataframe..")
    txn_df.show()

    # Show the DataFrame
    logger.info("Showing rates_df dataframe..")
    rates_df.show()

    # Step 1: Ensure txn_df's date is a string
    txn_df = txn_df.withColumn("Date", col("Date").cast("string"))

    # Step 2: Ensure rates_df's date is a string (usually already is, but just to be sure)
    rates_df = rates_df.withColumn("Date", col("Date").cast("string"))

    # Withdrawal Join
    txn_df_join = txn_df.join(
        rates_df,
        (txn_df["Date"] == rates_df["Date"]) &
        (txn_df["Withdrawal_currency"] == rates_df["Currency"]),
        "left"
    ).withColumnRenamed("Rate", "Withdrawal_rate") \
    .drop(rates_df["Date"]).drop("Currency")

    # Deposit Join
    txn_df_join = txn_df_join.join(
        rates_df,
        (txn_df["Date"] == rates_df["Date"]) &
        (txn_df["Deposit_currency"] == rates_df["Currency"]),
        "left"
    ).withColumnRenamed("Rate", "Deposit_rate") \
    .drop(rates_df["Date"]).drop("Currency")

    txn_df_join = txn_df_join.withColumn("normalized_withdrawal", when(col("Withdrawal_amt") == 0, 0).otherwise(col("Withdrawal_amt") / col("Withdrawal_rate")))

    txn_df_join = txn_df_join.withColumn("normalized_deposit",when(col("Deposit_amt") == 0, 0).otherwise(col("Deposit_amt") / col("Deposit_rate")))

    logger.info("Showing Final Dataframe..")
    
    txn_df_join.show()
    
    # ---------- Write to S3 as Parquet ----------
    txn_df_join.write.mode("overwrite") \
        .partitionBy("Date") \
        .parquet("s3://financial-data-pipeline-project/data/processed_data/")

    logger.info("✅ ETL pipeline completed successfully.")

except Exception as e:
    import traceback
    logger.error("[ERROR] Pipeline failed:\n" + traceback.format_exc())
