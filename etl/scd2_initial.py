# etl/scd2_initial.py
import pandas as pd
from datetime import datetime


def scd2_initial_load(engine, table_name: str):
    print(f"\n🚀 Initial load za {table_name}")

    # 1. Učitaj podatke iz ecommerce sheme
    try:
        df = pd.read_sql(f"SELECT * FROM ecommerce.{table_name}", engine)
    except Exception as e:
        print(f"❌ GRESKA: ecommerce.{table_name} ne postoji: {e}")
        return

    if df.empty:
        print(f"⚠️ ecommerce.{table_name} je prazan – preskačem")
        return

    # 2. Dodaj SCD2 kolone
    now = datetime.now()
    df["start_date"] = now
    df["end_date"] = "9999-12-31"
    df["updated"] = now
    df["process"] = "initial_load"

    # 3. Ubaci u archive shemu
    try:
        df.to_sql(table_name, con=engine, schema="archive", if_exists="append", index=False)
        print(f"✅ Ubaceno {len(df)} redova u archive.{table_name}")
    except Exception as e:
        print(f"❌ GRESKA prilikom ubacivanja u archive.{table_name}: {e}")
