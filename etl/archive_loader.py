from etl.scd2_upsert import scd2_upsert
import sqlalchemy as sa

engine = sa.create_engine("postgresql+psycopg2://postgres:alen@localhost:5432/ecommerce")

TABLES = {
    "customers": "customer_id",
    "sellers": "seller_id",
    "products": "product_id",
    "orders": "order_id",
    "order_reviews": "review_id",
    "order_payments": "order_id",
    "exchange_rates": "rate",
    "order_items": ["order_id", "order_item_id"]
}

def run_all_archive_loads():
    print("🔁 Incremental SCD2 upsert")
    for table, key in TABLES.items():
        try:
            scd2_upsert(engine, table, key)
        except Exception as e:
            print(f"❌ Greška u {table}: {e}")
