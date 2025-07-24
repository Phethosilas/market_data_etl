# Market Data ETL Pipeline Troubleshooting Guide

## Issue: Pipeline Only Inserting 1 Row Instead of Multiple Rows from 2025-01-01

### Root Cause Analysis

Your pipeline is actually working correctly, but there are several reasons why you might only see 1 row:

1. **API Endpoint Limitation**: The Polygon API endpoint `/v1/open-close/{symbol}/{date}` returns only **one record per day per symbol** - the daily OHLC (Open, High, Low, Close) data.

2. **Single Symbol Processing**: Your original DAG processes only one symbol (AAPL) per day.

3. **Catchup Not Triggered**: The backfill from 2025-01-01 might not have been triggered properly.

### Solutions Provided

#### 1. Fixed Original DAG (`market_etl.py`)
- Added proper DAG configuration
- Improved error handling
- Added better documentation

#### 2. Enhanced Load Function (`market_data_load.py`)
- Added proper data types (NUMERIC for prices, BIGINT for volume, DATE for dates)
- Implemented UPSERT functionality to handle duplicates
- Added unique constraint on (symbol, from_date)
- Better error handling and connection management

#### 3. Multi-Symbol DAG (`market_etl_multi_symbol.py`)
- Processes multiple symbols per day (AAPL, GOOGL, MSFT, TSLA, AMZN)
- Combines data from all symbols into a single load operation
- More efficient for getting multiple rows per day

### How to Get More Data

#### Option 1: Use Multi-Symbol DAG
The new `market_etl_multi_symbol.py` DAG will give you 5 rows per day (one for each symbol).

#### Option 2: Trigger Backfill Manually
If your original DAG hasn't backfilled from 2025-01-01, run this command in Airflow CLI:
```bash
airflow dags backfill -s 2025-01-01 -e 2025-01-23 market_etl
```

#### Option 3: Add More Symbols to Original DAG
Modify the original DAG to loop through multiple symbols:

```python
symbols = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]
for symbol in symbols:
    extract = market_data_extract(symbol=symbol, date="{{ ds }}")
    transform = polygon_response_to_df(extract)
    load = load_market_data(transform)
    extract >> transform >> load
```

#### Option 4: Use Intraday Data (if available)
If you need more granular data, consider using Polygon's intraday endpoints:
- `/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from}/{to}` for aggregated data
- `/v3/trades/{symbol}` for individual trades

### Expected Data Volume

With the current setup:
- **Original DAG**: 1 row per day (since 2025-01-01 = ~23 rows total)
- **Multi-Symbol DAG**: 5 rows per day (since 2025-01-01 = ~115 rows total)

### Database Schema

The improved load function creates a table with:
```sql
CREATE TABLE market_data (
    symbol TEXT,
    from_date DATE,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    afterHours NUMERIC,
    preMarket NUMERIC,
    volume BIGINT,
    status TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, from_date)
);
```

### Verification Steps

1. **Check DAG Status**: Ensure your DAG is enabled and running
2. **Check Logs**: Look at task logs for any API errors or data issues
3. **Verify Database**: Query your PostgreSQL table:
   ```sql
   SELECT COUNT(*) FROM market_data;
   SELECT symbol, from_date, close FROM market_data ORDER BY from_date DESC LIMIT 10;
   ```
4. **Check API Limits**: Ensure you're not hitting Polygon API rate limits

### Common Issues and Solutions

1. **No Data in Database**:
   - Check Airflow connection `polygon_api_postgres_conn`
   - Verify API key in `polygon_api` connection
   - Check task logs for errors

2. **API Errors**:
   - Verify API key is valid
   - Check if you're hitting rate limits
   - Ensure dates are weekdays (markets are closed on weekends)

3. **Duplicate Key Errors**:
   - The new UPSERT functionality handles this automatically
   - Old data will be updated instead of causing errors

### Next Steps

1. Enable the `market_etl_multi_symbol` DAG if you want multiple symbols
2. Trigger a backfill if needed
3. Monitor the pipeline for a few days to ensure it's working correctly
4. Consider adding more symbols or switching to intraday data if needed
