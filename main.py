from etl.load_data import create_tables_and_load_data
from etl.fetch_rates import fetch_and_insert_exchange_rates
from etl.archive_loader import run_all_archive_loads
from etl.etl_to_mssql import load_all_to_dwh
from etl.fact_loader import load_fact_order
from etl.currency_loader import load_dim_currency

if __name__ == "__main__":
    print("🚀 ETL pipeline started")

    # 1️⃣ CSV → staging PostgreSQL
    # create_tables_and_load_data()

    # 2️⃣ API → staging.exchange_rates
    # fetch_and_insert_exchange_rates()

    # 3️⃣ SCD2 → archive.*
    run_all_archive_loads()

    # 4️⃣ cleaned.* → MSSQL dim_* tabele
    # load_all_to_dwh()

    # 5️⃣ cleaned.* → MSSQL fact_order
    # load_fact_order()

    # 6️⃣ API → MSSQL dim_currency
    # load_dim_currency()

    print("✅ ETL pipeline completed successfully")
