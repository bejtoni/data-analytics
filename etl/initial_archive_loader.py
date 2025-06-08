from etl.initial_scd2 import scd2_initial_load
import sqlalchemy as sa
import psycopg2

# 🔌 SQLAlchemy konekcija na PostgreSQL bazu
engine = sa.create_engine("postgresql+psycopg2://postgres:alen@localhost:5432/ecommerce")

# 🧠 Tabele koje ćemo ubaciti iz staginga
TABLES = ["customers", "sellers", "products", "orders", "order_reviews", "order_payments", "order_items", "exchange_rates"]

# 🧱 1. Kreira archive shemu i tabele
def setup_archive_schema_only():
    print("📦 Kreiram archive shemu i tabele...")
    try:
        conn = psycopg2.connect(
            dbname="ecommerce",
            user="postgres",
            password="alen",
            host="localhost",
            port="5432"
        )
        cur = conn.cursor()
        with open("db/archive_schema.sql", "r", encoding="utf-8") as f:
            cur.execute(f.read())
            print("✅ Archive shema i tabele kreirane.")
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Greška pri kreiranju archive sheme: {e}")

# 🧽 2. Kreira cleaned view-ove nakon što su archive tabele napunjene
def setup_cleaned_views_only():
    print("🧼 Kreiram cleaned view-ove...")
    try:
        conn = psycopg2.connect(
            dbname="ecommerce",
            user="postgres",
            password="alen",
            host="localhost",
            port="5432"
        )
        cur = conn.cursor()
        with open("db/cleaned_schema.sql", "r", encoding="utf-8") as f:
            cur.execute(f.read())
            print("✅ Cleaned view-ovi kreirani.")
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Greška pri kreiranju cleaned view-ova: {e}")

# 🚀 3. Glavna funkcija za pokretanje initial loada
def run_initial_archive_loads():
    print("🚀 Pokrećem initial SCD2 load")

    # Prvo kreiraj archive shemu i prazne tabele
    setup_archive_schema_only()

    # Zatim ubaci podatke iz ecommerce staginga
    for table in TABLES:
        try:
            scd2_initial_load(engine, table)
        except Exception as e:
            print(f"❌ Greška u {table}: {e}")

    # Tek sada kreiraj cleaned view-ove
    setup_cleaned_views_only()

    print("✅ Initial SCD2 load završen.")
