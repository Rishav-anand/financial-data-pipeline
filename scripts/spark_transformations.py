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
    unfiltered_data = unfiltered_data.withColumn("Date",to_date("Date"))

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

    txn_df= txn_df.filter((txn_df["Year"] == year) & (txn_df["Month"] == month)& (txn_df["Day"] == day))

    # Show the DataFrame before filter
    logger.info("Showing txn_df dataframe after load..")
    txn_df.show()

    #Ensure txn_df's date is a correct format
    txn_df = txn_df.withColumn("Date", to_date("Date","dd-MM-yyyy"))

    # ---------- Fetch all exchange_rates.json file from s3 and convert into single df with respective dates ------------

    #It dynamically fectch all json one by one with its dates but this used when we need to proccess all files from s3 hence i am writing
    # code below to just load today's json file data
     
    # logger.info("🔄 Fetching exchange rates JSON files from S3...")
    # s3 = boto3.client('s3')
    # bucket_name = "financial-data-pipeline-project"
    # prefix = "data/exchange_rates/"
    # exchange_rates = {}

    # rate_files = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # for obj in rate_files.get("Contents", []):
    #     key = obj["Key"]
    #     if key.endswith(".json"):
    #         date = key.split("/")[-1].replace(".json", "")
    #         file = s3.get_object(Bucket=bucket_name, Key=key)
    #         rate_json = json.loads(file['Body'].read())
    #         exchange_rates[date] = rate_json.get("rates", {})
    
    # # Step 1: Flatten the dictionary into a list of rows
    # flattened_data = []
    # for date, currencies in exchange_rates.items():
    #     for currency, value in currencies.items():
    #         flattened_data.append((date, currency, float(value)))

    # # Step 2: Define schema
    # schema = StructType([
    #     StructField("date", StringType(), True),
    #     StructField("currency", StringType(), True),
    #     StructField("rate", FloatType(), True)
    # ])

    # # Step 3: Create DataFrame
    # rates_df = spark.createDataFrame(flattened_data, schema=schema)

    logger.info("🔄 Fetching exchange rates JSON files from S3...")
    today = date.today()

    df = spark.read.format("json")\
            .option('multiline','true')\
            .load(f's3://financial-data-pipeline-project/data/exchange_rates/{today}.json')
    logger.info("✅ Raw transaction data read successfully.")

    currency_list = df.select("rates").schema[0].dataType.names
    rows = df.collect()
    data=[]
    for row in rows:
        for currency in currency_list:
            value=row['rates'][currency]
            data.append(Row(date=today,currency=currency,rate=float(value)))
        
    rates_df = spark.createDataFrame(data)

    #Ensure txn_df's date is a correct format
    rates_df = rates_df.withColumn("date", to_date("date","dd-MM-yyyy"))
    
    # Show the DataFrame
    logger.info("Showing rates_df dataframe..")
    rates_df.show()

    # # Step 1: Ensure txn_df's date is a string
    # txn_df = txn_df.withColumn("Date", col("Date").cast("string"))

    # # Step 2: Ensure rates_df's date is a string (usually already is, but just to be sure)
    # rates_df = rates_df.withColumn("Date", col("Date").cast("string"))

    # Withdrawal Join
    txn_df_join = txn_df.join(
        broadcast(rates_df),
        (txn_df["Date"] == rates_df["Date"]) &
        (txn_df["Withdrawal_currency"] == rates_df["Currency"]),
        "left"
    ).withColumnRenamed("Rate", "Withdrawal_rate") \
    .drop(rates_df["Date"]).drop("Currency")

    # Deposit Join
    txn_df_join = txn_df_join.join(
        broadcast(rates_df),
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
