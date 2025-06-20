import pandas as pd
import numpy as np
import boto3
from datetime import date

today = pd.to_datetime(date.today())

df = pd.read_csv("s3://financial-data-pipeline-project/data/raw/raw_transaction_data.csv")
# print(df.head(1))

df['Date'] = pd.to_datetime(df['Date'])

#incremental load
df_filter_today = df[df['Date'].dt.date == today.date()]
print(df_filter_today)

#Integrity checks
# If Withdrawal_amt is not null, then Withdrawal_currency must not be null.
# If Deposit_amt is not null, then Deposit_currency must not be null.
# If violation is found → move the record to a rejected_data.csv file.

print("Missing Values of Withdrawal_amt before:",df_filter_today["Withdrawal_amt"].isnull().sum())
df_filter_today.loc[:, "Withdrawal_amt"] = df_filter_today["Withdrawal_amt"].fillna(0)
print("Missing Values of Withdrawal_amt after:",df_filter_today["Withdrawal_amt"].isnull().sum())

df_filter_today.loc[:,"Deposit_amt"] = df_filter_today["Deposit_amt"].fillna(0)

df_filter_today['flag1'] = np.where((df_filter_today["Withdrawal_amt"] != 0) & (df_filter_today["Withdrawal_currency"].isnull()),1,0)
df_filter_today['flag2'] = np.where((df_filter_today["Deposit_amt"] != 0) & (df_filter_today["Deposit_currency"].isnull()),1,0)

rejected_data = df_filter_today.loc[(df_filter_today["flag1"] == 1) | (df_filter_today["flag2"] == 1)]
rejected_data.drop(["flag1","flag2"],axis=1, inplace=True)
raw_transaction_cleaned = df_filter_today.loc[(df_filter_today["flag1"] == 0) & (df_filter_today["flag2"] == 0)]
raw_transaction_cleaned.drop(["flag1","flag2"],axis=1,inplace=True)

#pushing file to S3
rejected_data.to_csv("s3://financial-data-pipeline-project/data/rejected_data/rejected_data.csv")
raw_transaction_cleaned.to_csv("s3://financial-data-pipeline-project/data/cleaned_raw/raw_transaction_data.csv")
print("Write successful..")
    