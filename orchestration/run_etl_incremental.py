import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'etl')))

from etl.archive_loader import run_all_archive_loads
from etl.fact_loader import load_fact_order
from etl.fetch_rates import fetch_and_insert_exchange_rates
from etl.etl_to_mssql import load_all_to_dwh


def main():
    print("🚀 Starting INCREMENTAL ETL pipeline...")
    fetch_and_insert_exchange_rates()
    run_all_archive_loads()
    load_all_to_dwh()
    load_fact_order()
    print("✅ INCREMENTAL ETL pipeline completed.")

if __name__ == "__main__":
    main()
