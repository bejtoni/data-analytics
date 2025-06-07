import psycopg2
import pandas as pd
import os
from psycopg2.extras import execute_batch
import traceback

def load_csv_to_postgres(csv_path, table_name, conn):
    df = pd.read_csv(csv_path)
    df = df.where(pd.notnull(df), None)  # Zamijeni NaN sa None (kompatibilno s SQL)

    cursor = conn.cursor()
    columns = ','.join(df.columns)
    placeholders = ','.join(['%s'] * len(df.columns))
    sql = f'INSERT INTO ecommerce.{table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING'

    try:
        data = [tuple(row) for row in df.to_numpy()]
        execute_batch(cursor, sql, data, page_size=1000)
        conn.commit()
        print(f"✅ Učitan {table_name}")
    except Exception as e:
        print(f"❌ Greška u {table_name} prilikom batch unosa.")
        traceback.print_exc()

        print("🔍 Prebacujem na red po red za precizno debugovanje...")
        for i, row in df.iterrows():
            try:
                values = tuple(row)
                cursor.execute(sql, values)
            except Exception as single_error:
                print(f"‼️ Red {i + 2} u '{csv_path}' izazvao grešku:")
                print(f"    Vrijednosti: {values}")
                print(f"    Greška: {single_error}")
                break  # makni break ako želiš da nastavi dalje
        conn.rollback()


def create_tables_and_load_data():
    conn = psycopg2.connect(
        dbname="ecommerce",
        user="postgres",
        password="alen",  # 🔁 zamijeni svojom lozinkom
        host="localhost",
        port="5432"
    )

    # Kreiraj sve tabele
    with open("db/schema.sql", "r") as f:
        cursor = conn.cursor()
        cursor.execute(f.read())
        conn.commit()

    base_path = "data/raw"
    tables = {
        "customers": "olist_customers_dataset.csv",
        "sellers": "olist_sellers_dataset.csv",
        "orders": "olist_orders_dataset.csv",
        "order_items": "olist_order_items_dataset.csv",
        "products": "olist_products_dataset.csv",
        "product_category_name_translation": "product_category_name_translation.csv",
        "order_payments": "olist_order_payments_dataset.csv",
        "order_reviews": "olist_order_reviews_dataset.csv",
        "geolocation": "olist_geolocation_dataset.csv"
    }

    for table, filename in tables.items():
        full_path = os.path.join(base_path, filename)
        if not os.path.exists(full_path):
            print(f"⚠️ Fajl ne postoji: {full_path}")
            continue
        load_csv_to_postgres(full_path, table, conn)

    conn.close()


# import psycopg2
# import pandas as pd
# import os
#
#
# def load_csv_to_postgres(csv_path, table_name, conn):
#     df = pd.read_csv(csv_path)
#
#     df = df.where(pd.notnull(df), None)
#
#     cursor = conn.cursor()
#
#     for _, row in df.iterrows():
#         values = tuple(row.values)
#         placeholders = ','.join(['%s'] * len(values))
#         columns = ','.join(df.columns)
#         sql = f'INSERT INTO ecommerce.{table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING'
#         cursor.execute(sql, values)
#
#     conn.commit()
#     print(f"✅ Učitan {table_name}")
#
#
# def create_tables_and_load_data():
#     conn = psycopg2.connect(
#         dbname="ecommerce",
#         user="postgres",
#         password="alen",
#         host="localhost",
#         port="5432"
#     )
#
#     # Pokreni CREATE TABLE skriptu
#     with open("db/schema.sql", "r") as f:
#         cursor = conn.cursor()
#         cursor.execute(f.read())
#         conn.commit()
#
#     base_path = "data/raw"
#     tables = {
#         "customers": "olist_customers_dataset.csv",
#         "sellers": "olist_sellers_dataset.csv",
#         "orders": "olist_orders_dataset.csv",
#         "order_items": "olist_order_items_dataset.csv",
#         "products": "olist_products_dataset.csv",
#         "product_category_name_translation": "product_category_name_translation.csv",
#         "order_payments": "olist_order_payments_dataset.csv",
#         "order_reviews": "olist_order_reviews_dataset.csv",
#         "geolocation": "olist_geolocation_dataset.csv"
#     }
#
#     for table, filename in tables.items():
#         load_csv_to_postgres(os.path.join(base_path, filename), table, conn)
#
#     conn.close()


