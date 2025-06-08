# BI Project: ETL and Power BI Analysis on Brazilian E-commerce Dataset

## ✨ Introduction

This project implements a complete ETL architecture that integrates data from two sources:

1. CSV dataset from [Kaggle Brazil E-commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
2. Currency exchange rates via real-time API ([ExchangeRate-API](https://www.exchangerate-api.com/))

The goal of this project is to build a complete data pipeline that enables business analysis of Brazilian e-commerce transactions with integrated currency conversion. By combining order, product, and delivery data from a CSV dataset with real-time exchange rates from an API, the system tracks historical changes (via SCD2), structures the data in a star schema, and feeds Power BI dashboards with insights such as sales trends, top categories, and delivery performance — all dynamically adjustable by currency.

---

## 📂 Technologies

* **PostgreSQL** – used for staging, archiving, and cleaned layer (views)
* **MSSQL (bi\_dwh)** – used for star schema and Power BI connection
* **Python (pandas, SQLAlchemy, psycopg2)** – handles the entire ETL pipeline
* **Windows Task Scheduler** – for orchestration
* **Power BI** – for reporting and visualization

---

## ⚙️ ETL Architecture

```
CSV/API → staging (PostgreSQL.ecommerce)
        → archive (PostgreSQL.archive)
        → cleaned (PostgreSQL.cleaned - views)
        → star schema (MSSQL.bi_dwh)
        → Power BI
```

---

## 📃 Project Structure

```
project-root/
│
├── data/
│   ├── raw/                      # CSV files from Kaggle
│   └── api/                      # Exchange rate CSV files by timestamp
│
├── db/
│   ├── schema.sql                # ecommerce schema
│   ├── archive_schema.sql        # SCD2 archive tables
│   ├── cleaned_schema.sql        # Views on archive
│   └── insert.sql                # Sample insert statements
│
├── etl/
│   ├── staging_load_data.py         # CSV to staging database
│   ├── staging_fetch_rates.py       # API → staging
│   ├── initial_scd2.py              # Initial SCD2 row setup per table
│   ├── incremental_scd2_upsert.py   # Incremental SCD2 update per row
│   ├── initial_archive_loader.py    # Full archive+cleaned load
│   ├── incremental_archive_loader.py # Run SCD2 upserts on all tables
│   ├── incremental_insert_postgres.py  # Execute custom incremental SQL inserts
│   ├── mssql_create_fact_dim.py     # Create MSSQL dim/fact tables
│   ├── mssql_etl.py                 # Load dimensions
│   ├── mssql_fact_loader.py         # Load fact_order
│   └── utils.py                     # Timestamp formatting utilities
│
├── orchestration/
│   └── run_etl_incremental.py       # Run incremental ETL
│
└── main.py                          # Run full pipeline manually
```

---

## 🚀 Main ETL Flow (`main.py`)

1. Create staging tables and load CSV data
2. Fetch API and insert currency exchange rates
3. Initial SCD2 load into archive
4. Run `insert.sql` for sample data to demonstrate flow for incremental load
5. Incremental SCD2 upsert into archive
6. Create MSSQL `dim_*` and `fact_order` tables
7. Load `dim_*` tables from `cleaned.*` views
8. Load `fact_order` from multiple tables with joins/aggregations
9. Show analysis in Power BI when connected to the schema from `bi_dwh`

---

## ⏰ Orchestration: Windows Task Scheduler

`run_etl_incremental.bat`:

```bat
@echo off
cd C:\Users\Alen\PycharmProjects\PythonProject\data-analytics
call ..\.venv\Scripts\activate.bat
python orchestration\run_etl_incremental.py
```

Scheduler triggers this file daily.

---

## 🔁 SCD2 Logic

* `archive.*` tables include `start_date`, `end_date`, `updated`, `process`
* Each change closes the previous version (`end_date = NOW()`) and opens a new one (`end_date = '9999-12-31'`)
* `cleaned.*` is a view layer showing only active records

---

## 🔍 Power BI Pages

1. **Overview**: KPIs (order count, total value, averages)
2. **Orders Over Time**: Monthly trend via line chart
3. **Top Categories**: Top product categories + currency slicer
4. **Delivery Performance**: Shows monthly trend of delivered orders and overall delivery rate, with filters by year, state, and category.

Power BI connects directly to the MSSQL `bi_dwh` database.

---

## 📊 Conclusion

This project demonstrates a complete BI architecture with:

* Multi-source integration
* Historical tracking (SCD2)
* Clean view layer
* Star schema ready for reporting in Power BI
