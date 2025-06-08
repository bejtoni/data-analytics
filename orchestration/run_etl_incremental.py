import sys
import os

from etl.incremental_archive_loader import run_incremental_archive_loads

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'etl')))

from etl.mssql_fact_loader import load_fact_order
from etl.staging_fetch_rates import fetch_and_insert_exchange_rates
from etl.mssql_etl import load_all_to_dwh


def main():
    print("🚀 Starting INCREMENTAL ETL pipeline...")
    fetch_and_insert_exchange_rates()
    run_incremental_archive_loads()
    load_all_to_dwh()
    load_fact_order()
    print("✅ INCREMENTAL ETL pipeline completed.")

if __name__ == "__main__":
    main()
