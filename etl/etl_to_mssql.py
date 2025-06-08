import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text
from etl.utils import convert_timestamp_columns  # helper za datetime kolone

# 🔧 Konekcije

# PostgreSQL (staging, archive, cleaned)
pg_engine = sa.create_engine("postgresql+psycopg2://postgres:alen@localhost:5432/ecommerce")

# MSSQL DWH
mssql_conn_str = (
    "mssql+pyodbc://localhost/bi_dwh?"
    "driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)
engine = sa.create_engine(mssql_conn_str)

# 🔁 Helper za upis samo novih redova
def upsert_incremental(df, table_name, unique_cols):
    df = df.drop_duplicates(subset=unique_cols)

    with engine.connect() as conn:
        existing = pd.read_sql(f"SELECT {', '.join(unique_cols)} FROM dbo.{table_name}", conn)

    merged = df.merge(existing, how="left", on=unique_cols, indicator=True)
    new_rows = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

    if not new_rows.empty:
        new_rows.to_sql(table_name, con=engine, schema="dbo", if_exists="append", index=False)
        print(f"✅ Ubaceno {len(new_rows)} novih redova u {table_name}.")
    else:
        print(f"ℹ️ Nema novih redova za {table_name}.")

# ✅ DIM_CUSTOMER
def load_dim_customer():
    df = pd.read_sql(
        "SELECT customer_id, customer_city, customer_state FROM cleaned.customers",
        pg_engine
    )
    df.columns = ['customer_id', 'city', 'state']
    df = convert_timestamp_columns(df)
    upsert_incremental(df, "dim_customer", ["customer_id"])

# ✅ DIM_PRODUCT
def load_dim_product():
    query = """
    SELECT 
        p.product_id,
        t.product_category_name_english AS category,
        p.product_weight_g,
        p.product_name_lenght,
        p.product_description_lenght
    FROM cleaned.products p
    LEFT JOIN ecommerce.product_category_name_translation t
        ON p.product_category_name = t.product_category_name
    """
    df = pd.read_sql(query, pg_engine)
    df.columns = ['product_id', 'category', 'weight_g', 'name_length', 'description_length']
    df = convert_timestamp_columns(df)
    upsert_incremental(df, "dim_product", ["product_id"])

# ✅ DIM_SELLER
def load_dim_seller():
    df = pd.read_sql(
        "SELECT seller_id, seller_city, seller_state FROM cleaned.sellers",
        pg_engine
    )
    df.columns = ['seller_id', 'city', 'state']
    df = convert_timestamp_columns(df)
    upsert_incremental(df, "dim_seller", ["seller_id"])

# ✅ DIM_DATE (uvijek se re-kreira jer je kalendar)
def load_dim_date():
    df = pd.read_sql(
        "SELECT DISTINCT order_purchase_timestamp AS date FROM cleaned.orders",
        pg_engine
    )
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
    df = df.drop_duplicates(subset=["date_key", "base_currency", "target_currency"])

    # Kreiraj tabelu ako ne postoji
    with engine.connect() as conn:
        conn.execute(text("""
            IF NOT EXISTS (
                SELECT * FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'dim_currency'
            )
            BEGIN
                CREATE TABLE dbo.dim_currency (
                    date_key DATETIME,
                    base_currency VARCHAR(10),
                    target_currency VARCHAR(10),
                    rate DECIMAL(10, 5),
                    PRIMARY KEY (date_key, base_currency, target_currency)
                )
            END
        """))
        conn.commit()

    upsert_incremental(df, "dim_currency", ["date_key", "base_currency", "target_currency"])

# ✅ Glavni ulaz
def load_all_to_dwh():
    load_dim_customer()
    load_dim_product()
    load_dim_seller()
    load_dim_date()
    load_dim_currency()
    print("✅ All DWH dimensions loaded")
