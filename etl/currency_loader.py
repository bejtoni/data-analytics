import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text
import glob
import os

# 🔧 MSSQL engine
mssql_engine = sa.create_engine(
    "mssql+pyodbc://localhost/bi_dwh?"
    "driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)


# ✅ Kreiraj dim_currency ako ne postoji
def create_dim_currency_if_not_exists(engine):
    with engine.connect() as conn:
        conn.execute(text("""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = 'dim_currency'
        )
        BEGIN
            CREATE TABLE dim_currency (
                date_key DATETIME,
                base_currency VARCHAR(10),
                target_currency VARCHAR(10),
                rate DECIMAL(10, 5),
                PRIMARY KEY (date_key, base_currency, target_currency)
            )
        END
        """))
        print("✅ dim_currency tabela postoji ili je kreirana.")


# ✅ Učitaj i ubaci najnoviji exchange_rates CSV u MSSQL
def load_dim_currency():
    print("⏳ Loading dim_currency...")

    # 1. Kreiraj tabelu ako ne postoji
    create_dim_currency_if_not_exists(mssql_engine)

    # 2. Pronađi najnoviji CSV fajl
    files = sorted(glob.glob("data/api/exchange_rates_*.csv"), reverse=True)
    if not files:
        print("❌ Nema exchange_rates_*.csv fajlova.")
        return

    csv_path = files[0]
    print(f"📂 Koristi se fajl: {os.path.basename(csv_path)}")

    # 3. Učitaj CSV
    df = pd.read_csv(csv_path)
    df = df.where(pd.notnull(df), None)

    # 4. Preimenuj kolone ako su u starom formatu
    df.columns = [col.lower() for col in df.columns]
    if 'date' in df.columns:
        df.rename(columns={
            "date": "date_key",
            "base": "base_currency",
            "target": "target_currency"
        }, inplace=True)

    df["date_key"] = pd.to_datetime(df["date_key"], errors="coerce")

    # 5. Učitaj već postojeće redove
    with mssql_engine.connect() as conn:
        existing = pd.read_sql(
            "SELECT date_key, base_currency, target_currency FROM dim_currency",
            conn
        )

    # 6. Uporedi i filtriraj samo nove redove
    merged = df.merge(
        existing,
        how="left",
        on=["date_key", "base_currency", "target_currency"],
        indicator=True
    )

    new_rows = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

    # 7. Ubaci u MSSQL
    if not new_rows.empty:
        new_rows.to_sql("dim_currency", con=mssql_engine, if_exists="append", index=False)
        print(f"✅ dim_currency loaded ({len(new_rows)} novih redova)")
    else:
        print("ℹ️ Nema novih redova za dim_currency")
