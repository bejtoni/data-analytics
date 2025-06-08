import pandas as pd
import sqlalchemy as sa
from etl.utils import convert_timestamp_columns
from sqlalchemy import text

# 🔧 CONNECTIONS
pg_engine = sa.create_engine("postgresql+psycopg2://postgres:alen@localhost:5432/ecommerce")
mssql_engine = sa.create_engine(
    "mssql+pyodbc://localhost/bi_dwh?"
    "driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)

# ✅ Kreiraj tabelu ako ne postoji
def create_fact_order_table_if_not_exists():
    with mssql_engine.connect() as conn:
        conn.execute(text("""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'fact_order'
        )
        BEGIN
            CREATE TABLE fact_order (
                order_id VARCHAR(50) PRIMARY KEY,
                customer_id VARCHAR(50),
                date_key DATETIME,
                seller_id VARCHAR(50),
                total_payment_value DECIMAL(10, 2),
                total_freight_value DECIMAL(10, 2),
                product_count INT,
                review_score INT,
                order_status VARCHAR(20)
            )
        END
        """))
        print("✅ fact_order tabela postoji ili je kreirana.")

# ✅ Load fact_order from cleaned.*
def load_fact_order():
    print("⏳ Loading fact_order...")
    create_fact_order_table_if_not_exists()

    # 1. Čitaj očišćene podatke
    orders = pd.read_sql("SELECT * FROM cleaned.orders", pg_engine)
    items = pd.read_sql("SELECT * FROM cleaned.order_items", pg_engine)
    payments = pd.read_sql("SELECT * FROM cleaned.order_payments", pg_engine)
    reviews = pd.read_sql("SELECT * FROM cleaned.order_reviews", pg_engine)

    # 2. Standardizuj vrijeme
    orders = convert_timestamp_columns(orders)
    items = convert_timestamp_columns(items)
    payments = convert_timestamp_columns(payments)
    reviews = convert_timestamp_columns(reviews)

    # 3. Agregacije
    payment_agg = payments.groupby("order_id", as_index=False).agg(
        total_payment_value=("payment_value", "sum")
    )

    item_agg = items.groupby("order_id", as_index=False).agg(
        total_freight_value=("freight_value", "sum"),
        product_count=("order_item_id", "count"),
        seller_id=("seller_id", "first")
    )

    review_agg = reviews[["order_id", "review_score"]].drop_duplicates()

    # 4. Spoji sve u finalni DataFrame
    df = (
        orders[["order_id", "customer_id", "order_status", "order_purchase_timestamp"]]
        .merge(payment_agg, on="order_id", how="left")
        .merge(item_agg, on="order_id", how="left")
        .merge(review_agg, on="order_id", how="left")
    )

    df = df.rename(columns={"order_purchase_timestamp": "date_key"})
    df.drop_duplicates(subset=["order_id"], inplace=True)

    # 5. Upis u MSSQL
    df.to_sql("fact_order", con=mssql_engine, if_exists="replace", index=False)
    print("✅ fact_order loaded")
