from etl.incremental_archive_loader import run_incremental_archive_loads
from etl.incremental_insert_postgres import execute_sql_from_file
from etl.mssql_create_fact_dim import create_all_star_schema_tables
from etl.mssql_etl import load_all_to_dwh
from etl.mssql_fact_loader import load_fact_order
from etl.staging_fetch_rates import fetch_and_insert_exchange_rates
from etl.initial_archive_loader import run_initial_archive_loads
from etl.staging_load_data import create_tables_and_load_data


if __name__ == "__main__":
    print("🚀 ETL pipeline started")

    # 1️⃣ CSV → staging PostgreSQL
    print("Step 1: Creating tables and loading data from CSV to staging PostgreSQL...")
    # create_tables_and_load_data()

    # 2️⃣ API → staging.exchange_rates
    print("Step 2: Fetching and inserting exchange rates into staging...")
    # fetch_and_insert_exchange_rates()

    # 3️⃣ SAMO INITIAL LOAD -> archive
    print("Step 3: Running initial archive load...")
    # run_initial_archive_loads()

    # 4️⃣ INSERT SQL skriptu prije incremental load
    print("Step 4: Running incremental insert SQL script...")
    sql_file_path = "db/insert.sql"
    execute_sql_from_file(sql_file_path)

    # 5️⃣ SCD2 → archive.* (INCREMENTAL)
    print("Step 5: Running insert + incremental archive load...")
    run_incremental_archive_loads()

    # 6️⃣ Create Star Schema Tables
    print("Step 6: Creating all star schema tables...")
    # create_all_star_schema_tables()

    # 7️⃣ cleaned.* → MSSQL dim_* tables
    print("Step 7: Loading cleaned data into MSSQL dimension tables...")
    load_all_to_dwh()

    # 8️⃣ cleaned.* → MSSQL fact_order
    print("Step 8: Loading cleaned data into MSSQL fact_order table...")
    load_fact_order()

    print("✅ ETL pipeline completed successfully")
