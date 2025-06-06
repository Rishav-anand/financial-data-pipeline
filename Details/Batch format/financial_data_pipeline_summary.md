# 🏗️ AWS-Based Financial Data Pipeline Project — End-to-End Summary

_Last Updated: 2025-06-01_

---

## ✅ Step-by-Step Breakdown

### 🔹 Step 1: Prepare Transaction Data (Input)

- Generated a synthetic transaction dataset `raw_transaction_data.csv` with fields:
  - `Account_ID`, `Date`, `Withdrawal_amt`, `Withdrawal_currency`, `Deposit_amt`, `Deposit_currency`, etc.
- Ensured the `Date` field ranges from **`2023-01-01` to `2025-05-07`**
- Uploaded to S3:
  ```text
  s3://financial-data-pipeline-project/raw_data/raw_transaction_data.csv
  ```

---

### 🔹 Step 2: Real-time Exchange Rate Ingestion Using EC2

- Created `fetch_exchange_rates.py` which:
  - Hits exchange rate API daily
  - Saves response in `exchange_rates/YYYY-MM-DD.json`
- EC2 instance:
  - Hosted script with boto3, cron, IAM role
  - Output written to:
    ```text
    s3://financial-data-pipeline-project/exchange_rates/YYYY-MM-DD.json
    ```

---

### 🔹 Step 3: Launch EMR Cluster & Submit ETL Job

- EMR configured with:
  - Spark enabled
  - IAM Roles: `EMR_DefaultRole`, `EMR_EC2_DefaultRole`
- Uploaded PySpark script to:
  ```text
  s3://financial-data-pipeline-project/scripts/dummp.py
  ```
- Submitted step:
  ```bash
  spark-submit --deploy-mode cluster s3://.../dummp.py
  ```

---

### 🔹 Step 4: PySpark ETL Script Overview

- Reads raw CSV from S3 into Spark DataFrame
- Loads exchange rate JSONs from `exchange_rates/` prefix
- Broadcasts the exchange rate dictionary
- Defines UDF `normalize_currency` to convert to SGD
- Adds columns `normalized_withdrawal`, `normalized_deposit`
- Writes processed Parquet to:
  ```text
  s3://financial-data-pipeline-project/processed_data/
  ```

---

### 🔹 Step 5: Logging, Monitoring, and Debugging

- Used Python `logging` with `py4j` configuration:
  ```python
  logger = logging.getLogger("py4j")
  logger.setLevel(logging.INFO)
  ```
- Print statements replaced with logger calls
- Output visible in Spark History Server or EMR logs

---

### 🔹 Step 6: AWS Glue & Athena

- Ran AWS Glue Crawler on `processed_data/` to create table
- Queried normalized data in Athena:
  ```sql
  SELECT * FROM financial_data_db.processed_data
  WHERE Date = '2024-12-01'
  ORDER BY normalized_withdrawal DESC;
  ```

---

## 🔧 Additional Enhancements & Fixes

- Fixed DateType mismatch (`String` vs `Date`)
- UDF logging helped debug missing/invalid exchange rates
- Resolved EMR step failures (IAM permissions, boto3 installation)
- Validated final data with `.show()` and Athena queries

---

_This project demonstrates real-time currency normalization of financial transactions using Spark, S3, EMR, Glue, and Athena._
