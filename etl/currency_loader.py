import pandas as pd
import glob
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# 🔧 MSSQL engine (bi_dwh database)
# mssql_engine = create_engine(
#     "mssql+pyodbc://localhost/bi_dwh?driver=ODBC+Driver+17+for+SQL+Server;Trusted_Connection=yes"
# )


connection_url = URL.create(
    "mssql+pyodbc",
    username=None,
    password=None,
    host="localhost",
    database="bi_dwh",
    query={
        "driver": "ODBC Driver 17 for SQL Server",
        "Trusted_Connection": "yes"
    }
)

mssql_engine = create_engine(connection_url)


# ✅ Kreiraj dbo.dim_currency ako ne postoji
def create_dim_currency_if_not_exists(engine):
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
        print("✅ dbo.dim_currency tabela postoji ili je kreirana.")


# ✅ Učitaj i upiši najnoviji CSV u dbo.dim_currency
def load_dim_currency():
    print("⏳ Loading dim_currency...")

    # 1. Pronađi najnoviji exchange_rates_*.csv fajl
    files = sorted(glob.glob("data/api/exchange_rates_*.csv"), reverse=True)
    if not files:
        print("❌ Nema exchange_rates_*.csv fajlova.")
        return

    csv_path = files[0]
    print(f"📂 Koristi se fajl: {os.path.basename(csv_path)}")

    # 2. Učitaj CSV i preimenuj kolone ako treba
    df = pd.read_csv(csv_path)
    df.columns = [c.lower() for c in df.columns]

    if "date" in df.columns:
        df.rename(columns={
            "date": "date_key",
            "base": "base_currency",
            "target": "target_currency"
        }, inplace=True)

    df["date_key"] = pd.to_datetime(df["date_key"], errors="coerce")
    df = df[["date_key", "base_currency", "target_currency", "rate"]]

    # 3. Kreiraj tabelu ako ne postoji
    create_dim_currency_if_not_exists(mssql_engine)

    # 4. Pročitaj već postojeće redove
    with mssql_engine.connect() as conn:
        existing = pd.read_sql(
            "SELECT date_key, base_currency, target_currency FROM dbo.dim_currency",
            conn
        )

    # 5. Nađi samo nove redove
    merged = df.merge(
        existing,
        how="left",
        on=["date_key", "base_currency", "target_currency"],
        indicator=True
    )
    new_rows = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

    # 6. Ubaci samo nove redove
    if not new_rows.empty:
        new_rows.to_sql(
            "dim_currency",
            con=mssql_engine,
            schema="dbo",
            if_exists="append",
            index=False
        )
        print(f"✅ Ubaceno {len(new_rows)} novih redova u dbo.dim_currency.")
    else:
        print("ℹ️ Nema novih redova za ubacivanje.")
