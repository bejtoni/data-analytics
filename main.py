from etl.currency_loader import load_dim_currency
from etl.fact_loader import load_fact_order
from etl.fetch_rates import fetch_exchange_rate, insert_exchange_rates_to_db
from etl.load_data import create_tables_and_load_data
from etl.etl_to_mssql import load_all_to_dwh

if __name__ == "__main__":
    # create_tables_and_load_data()
    # fetch_exchange_rate()
    # load_all_to_dwh()
    # load_fact_order()
    load_dim_currency()