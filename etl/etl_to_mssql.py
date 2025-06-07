import pandas as pd
import sqlalchemy as sa
from etl.utils import convert_timestamp_columns  # ← tvoj helper

# 🔧 CONNECTIONS

# PostgreSQL (SQLAlchemy konekcija — više nema warninga)
pg_engine = sa.create_engine("postgresql+psycopg2://postgres:alen@localhost:5432/ecommerce")

# MSSQL DWH (trusted_connection=yes)
mssql_conn_str = (
    "mssql+pyodbc://localhost/bi_dwh?"
    "driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)
engine = sa.create_engine(mssql_conn_str)


# ✅ DIM_CUSTOMER
def load_dim_customer():
    df = pd.read_sql(
        "SELECT customer_id, customer_city, customer_state FROM ecommerce.customers",
        pg_engine
    )
    df.columns = ['customer_id', 'city', 'state']
    df.drop_duplicates(subset=['customer_id'], inplace=True)
    df = convert_timestamp_columns(df)
    df.to_sql("dim_customer", con=engine, if_exists="replace", index=False)
    print("✅ dim_customer loaded")


# ✅ DIM_PRODUCT
def load_dim_product():
    query = """
    SELECT 
        p.product_id,
        t.product_category_name_english AS category,
        p.product_weight_g,
        p.product_name_lenght,
        p.product_description_lenght
    FROM ecommerce.products p
    LEFT JOIN ecommerce.product_category_name_translation t
        ON p.product_category_name = t.product_category_name
    """
    df = pd.read_sql(query, pg_engine)
    df.columns = ['product_id', 'category', 'weight_g', 'name_length', 'description_length']
    df.drop_duplicates(subset=['product_id'], inplace=True)
    df = convert_timestamp_columns(df)
    df.to_sql("dim_product", con=engine, if_exists="replace", index=False)
    print("✅ dim_product loaded")


# ✅ DIM_SELLER
def load_dim_seller():
    df = pd.read_sql(
        "SELECT seller_id, seller_city, seller_state FROM ecommerce.sellers",
        pg_engine
    )
    df.columns = ['seller_id', 'city', 'state']
    df.drop_duplicates(subset=['seller_id'], inplace=True)
    df = convert_timestamp_columns(df)
    df.to_sql("dim_seller", con=engine, if_exists="replace", index=False)
    print("✅ dim_seller loaded")


# ✅ DIM_DATE
def load_dim_date():
    df = pd.read_sql(
        "SELECT DISTINCT order_purchase_timestamp AS date FROM ecommerce.orders",
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




# ✅ ENTRY POINT
def load_all_to_dwh():
    load_dim_customer()
    load_dim_product()
    load_dim_seller()
    load_dim_date()
    print("✅ All DWH dimensions loaded.")
