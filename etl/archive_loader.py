from sqlalchemy import create_engine, text
from etl.scd2_upsert import scd2_upsert

pg_engine = create_engine("postgresql+psycopg2://postgres:alen@localhost:5432/ecommerce")

tables = [
    ("customers", "customer_id", ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"]),
    ("sellers", "seller_id", ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"]),
    ("products", "product_id", ["product_id", "product_category_name", "product_name_lenght", "product_description_lenght", "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]),
    ("orders", "order_id", ["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"]),
    ("order_reviews", "review_id", ["review_id", "order_id", "review_score", "review_comment_title", "review_comment_message", "review_creation_date", "review_answer_timestamp"]),
    ("order_payments", "order_id", ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"]),
]

def run_all_archive_loads():
    with pg_engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS archive;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS cleaned;"))
        print("✅ Šeme 'archive' i 'cleaned' su kreirane (ako već nisu).")

    for table_name, key_column, all_columns in tables:
        with pg_engine.begin() as conn:
            create_stmt = f"""
            CREATE TABLE IF NOT EXISTS archive.{table_name} (
                {", ".join([f"{col} TEXT" for col in all_columns])},
                start_date TIMESTAMP,
                end_date TIMESTAMP
            );
            """

            conn.execute(text(create_stmt))
            print(f"📦 Kreirana tabela archive.{table_name} (ako nije postojala)")

        scd2_upsert(pg_engine, table_name, key_column, all_columns)

        with pg_engine.begin() as conn:
            view_stmt = f"""
            CREATE OR REPLACE VIEW cleaned.{table_name} AS
            SELECT {', '.join(all_columns)}
            FROM archive.{table_name}
            WHERE end_date = '9999-12-31';
            """
            conn.execute(text(view_stmt))
            print(f"✨ Kreiran view cleaned.{table_name}")
