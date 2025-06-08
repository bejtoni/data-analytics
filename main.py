from etl.create_star_schema_tables import create_all_star_schema_tables
from etl.initial_archive_loader import run_initial_archive_loads
from etl.load_data import create_tables_and_load_data
from etl.fetch_rates import fetch_and_insert_exchange_rates
from etl.archive_loader import run_all_archive_loads
from etl.etl_to_mssql import load_all_to_dwh
from etl.fact_loader import load_fact_order

if __name__ == "__main__":
    print("🚀 ETL pipeline started")

    # 1️⃣ CSV → staging PostgreSQL
    # create_tables_and_load_data()

    # 2️⃣ API → staging.exchange_rates
    # fetch_and_insert_exchange_rates()

    # 3️⃣ SAMO INITIAL LOAD
    # run_initial_archive_loads()

    # # 3️⃣ SCD2 → archive.*  INCREMNTAL
    # run_all_archive_loads()

    create_all_star_schema_tables()

    # 4️⃣ cleaned.* → MSSQL dim_* tabele
    load_all_to_dwh()

    # 5️⃣ cleaned.* → MSSQL fact_order
    load_fact_order()

    print("✅ ETL pipeline completed successfully")
