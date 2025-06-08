import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text
from etl.utils import convert_timestamp_columns

# 🔧 CONNECTIONS
pg_engine = sa.create_engine("postgresql+psycopg2://postgres:alen@localhost:5432/ecommerce")
mssql_engine = sa.create_engine(
    "mssql+pyodbc://localhost/bi_dwh?"
    "driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)

# ✅ Učitaj iz cleaned i dodaj samo nove redove u fact_order
def load_fact_order():
    print("⏳ Loading fact_order ...")

    # 1. Učitaj podatke iz cleaned sheme
    orders = pd.read_sql("SELECT * FROM cleaned.orders", pg_engine)
    items = pd.read_sql("SELECT * FROM cleaned.order_items", pg_engine)
    payments = pd.read_sql("SELECT * FROM cleaned.order_payments", pg_engine)
    reviews = pd.read_sql("SELECT * FROM cleaned.order_reviews", pg_engine)

    # 2. Standardizuj datetime kolone
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
        seller_id=("seller_id", "first"),
        product_id=("product_id", "first")  # ili koristi .mode()[0] ako želiš najčešći
    )

    review_agg = reviews[["order_id", "review_score"]].drop_duplicates()

    # 4. Merge u finalni DataFrame
    df = (
        orders[["order_id", "customer_id", "order_status", "order_purchase_timestamp"]]
        .merge(payment_agg, on="order_id", how="left")
        .merge(item_agg, on="order_id", how="left")
        .merge(review_agg, on="order_id", how="left")
    )
    df = df.rename(columns={"order_purchase_timestamp": "date_key"})
    df.drop_duplicates(subset=["order_id"], inplace=True)

    # 5. Detekcija već postojećih redova
    with mssql_engine.connect() as conn:
        existing = pd.read_sql("SELECT order_id FROM fact_order", conn)

    merged = df.merge(existing, how="left", on="order_id", indicator=True)
    new_rows = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

    # 6. Insert samo novih redova
    if not new_rows.empty:
        new_rows.to_sql("fact_order", con=mssql_engine, if_exists="append", index=False)
        print(f"✅ Ubaceno {len(new_rows)} novih redova u fact_order.")
    else:
        print("ℹ️ Nema novih redova za fact_order.")
