import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text
from etl.utils import convert_timestamp_columns  # helper za datetime kolone

# 🔧 Konekcije
pg_engine = sa.create_engine("postgresql+psycopg2://postgres:alen@localhost:5432/ecommerce")

mssql_conn_str = (
    "mssql+pyodbc://localhost/bi_dwh?"
    "driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)
engine = sa.create_engine(mssql_conn_str)


# 🔁 Helper: potpuno obriši i ubaci nove redove
def replace_dim_table(df, table_name: str):
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE dbo.{table_name}"))
    df.to_sql(table_name, con=engine, schema="dbo", if_exists="append", index=False)
    print(f"✅ {table_name} reloaded sa {len(df)} redova.")


# ✅ DIM_CUSTOMER
def load_dim_customer():
    df = pd.read_sql("""
        SELECT customer_id, customer_city AS city, customer_state AS state
        FROM cleaned.customers
    """, pg_engine)
    df = convert_timestamp_columns(df)
    replace_dim_table(df, "dim_customer")


# ✅ DIM_PRODUCT
def load_dim_product():
    df = pd.read_sql("""
        SELECT 
            p.product_id,
            t.product_category_name_english AS category,
            p.product_weight_g AS weight_g,
            p.product_name_lenght AS name_length,
            p.product_description_lenght AS description_length
        FROM cleaned.products p
        LEFT JOIN ecommerce.product_category_name_translation t
            ON p.product_category_name = t.product_category_name
    """, pg_engine)
    df = convert_timestamp_columns(df)
    replace_dim_table(df, "dim_product")


# ✅ DIM_SELLER
def load_dim_seller():
    df = pd.read_sql("""
        SELECT seller_id, seller_city AS city, seller_state AS state
        FROM cleaned.sellers
    """, pg_engine)
    df = convert_timestamp_columns(df)
    replace_dim_table(df, "dim_seller")


# ✅ DIM_DATE (uvijek se rekreira)
def load_dim_date():
    df = pd.read_sql("""
        SELECT DISTINCT order_purchase_timestamp AS date FROM cleaned.orders
    """, pg_engine)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day
    df['hour'] = df['date'].dt.hour
    df['minute'] = df['date'].dt.minute
    df['second'] = df['date'].dt.second
    df['weekday_name'] = df['date'].dt.day_name()
    df.columns = ['date_key', 'year', 'month', 'day', 'hour', 'minute', 'second', 'weekday_name']
    df.drop_duplicates(subset=['date_key'], inplace=True)

    df.to_sql("dim_date", con=engine, if_exists="replace", index=False)
    print("✅ dim_date loaded")


# ✅ DIM_CURRENCY
def load_dim_currency():
    print("⏳ Loading dim_currency...")
    df = pd.read_sql("""
        SELECT date AS date_key, base_currency, target_currency, rate
        FROM cleaned.exchange_rates
    """, pg_engine)

    df["date_key"] = pd.to_datetime(df["date_key"], errors="coerce")
    df.drop_duplicates(subset=["date_key", "base_currency", "target_currency"], inplace=True)
    replace_dim_table(df, "dim_currency")


# ✅ Glavna funkcija
def load_all_to_dwh():
    print("🚀 ETL: Loading all DWH dimensions...")
    load_dim_customer()
    load_dim_product()
    load_dim_seller()
    load_dim_date()
    load_dim_currency()
    print("✅ All DWH dimensions loaded")

