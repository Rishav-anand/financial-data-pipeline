# Currency-Normalized Financial Transactions Pipeline Project

## 📌 What the Project Is About

This project is built for banks and fintech companies that deal with customers making international money transfers or using multiple currencies (like USD, INR, SGD).

Imagine someone who travels a lot or does business in different countries. Their bank transactions will be in different currencies. But when banks need to evaluate their financial health (for giving loans, for example), it's hard to compare all that mixed-currency data.

### 🎯 Main Goal

To convert all transactions into one single currency (SGD) so it's easier to analyze and make decisions — like whether to approve a loan or not.

## 🔁 How It Works

1. **Get Data**: Bank transaction data (in different currencies) is collected.
2. **Fetch Real-time Exchange Rates**: Exchange rates are fetched from an API.
3. **Process Data**: Data is processed and normalized using a cloud pipeline on AWS.

## 📂 Data Description

This project uses two primary data sources:

### 1. Exchange Rate API (Open Exchange Rates)

The first data source is an API from Open Exchange Rates. It provides exchange rate data for various currencies, including historical data.

#### 📊 Example API Response

URL: `https://openexchangerates.org/api/historical/2024-05-01.json?app_id=YOUR_API_KEY&base=USD`

```json
{
  "base": "USD",
  "date": "2024-05-01",
  "rates": {
    "SGD": 1.34,
    "INR": 83.21,
    "EUR": 0.92,
    ...
  }
}
```

#### 💱 Example Use Case

Let’s say a person:
- Withdraws **$100 USD** on **2024-05-01**
- We want to convert this to SGD.

Using the API:
- Exchange rate on 2024-05-01 → **1 USD = 1.34 SGD**
- Normalized amount → `100 × 1.34 = 134 SGD`

### 2. Raw Transaction Data (CSV)

The second data source is a static CSV file with customer transaction data. Columns include:

- **Account_ID**: Bank account identifier
- **Date**: Transaction date
- **Transaction_details**: Description of the transaction
- **Chq_no**: Cheque number (if used)
- **Value_date**: Date when transaction was processed
- **Withdrawal_amt / Deposit_amt**: Amount withdrawn or deposited
- **Withdrawal_currency / Deposit_currency**: Currency used
- **Balance_amt**: Balance after transaction