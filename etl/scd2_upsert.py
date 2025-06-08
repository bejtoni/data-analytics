import pandas as pd
from datetime import datetime
from sqlalchemy import text

def scd2_upsert(engine, table_name: str, key_column: str):
    print(f"\n🔁 START: SCD2 upsert za tabelu: {table_name}")
    now = datetime.now()

    # 1. Učitaj nove podatke iz STAGING (ecommerce)
    new_df = pd.read_sql(f"SELECT * FROM ecommerce.{table_name}", engine).copy()
    print(f"🟢 {len(new_df)} redova u new_df")

    # 2. Učitaj stare podatke iz ARCHIVE
    try:
        old_df = pd.read_sql(f"SELECT * FROM archive.{table_name}", engine).copy()
        print(f"🔵 {len(old_df)} redova u old_df")
    except Exception as e:
        print(f"⚠️ Nema archive.{table_name}: {e}")
        old_df = pd.DataFrame()

    # 3. Ako je arhiva prazna – sve je novo
    if old_df.empty:
        new_df["start_date"] = now
        new_df["end_date"] = "9999-12-31"
        new_df["updated"] = now
        new_df["process"] = "initial_or_first_incremental"

        new_df.to_sql(table_name, con=engine, schema="archive", if_exists="append", index=False)
        print(f"✅ Ubaceno {len(new_df)} redova u archive.{table_name}")
        return

    # 4. Poređenje (isključujemo sistemske kolone)
    exclude_cols = ["start_date", "end_date", "updated", "process"]
    compare_cols = [col for col in new_df.columns if col not in exclude_cols]

    merged = pd.merge(new_df[compare_cols], old_df[compare_cols], how="left", indicator=True)
    new_versions = new_df.loc[merged['_merge'] == 'left_only'].copy()
    print(f"🆕 {len(new_versions)} novih/redigovanih redova")

    if new_versions.empty:
        print(f"✅ Nema promjena za {table_name}")
        return

    # 5. Dodaj sistemske kolone
    new_versions["start_date"] = now
    new_versions["end_date"] = "9999-12-31"
    new_versions["updated"] = now
    new_versions["process"] = "incremental_upsert"

    with engine.begin() as conn:
        # 6. Specijalna logika za exchange_rates
        if table_name == "exchange_rates":
            for _, row in new_versions.iterrows():
                conn.execute(text(f"""
                    UPDATE archive.exchange_rates
                    SET end_date = :now
                    WHERE target_currency = :target
                    AND end_date = '9999-12-31'
                """), {
                    "now": now,
                    "target": row["target_currency"]
                })
        else:
            # Standardni slučaj – koristi key_column (npr. customer_id)
            if isinstance(key_column, list):  # Composite key (e.g., order_items)
                key_combos = new_versions[key_column].drop_duplicates().values.tolist()
                for combo in key_combos:
                    where_clause = " AND ".join([f"{col} = :{col}" for col in key_column])
                    params = {col: val for col, val in zip(key_column, combo)}
                    params["now"] = now
                    conn.execute(text(f"""
                        UPDATE archive.{table_name}
                        SET end_date = :now
                        WHERE {where_clause}
                        AND end_date = '9999-12-31'
                    """), params)
            else:
                keys_to_close = new_versions[key_column].unique().tolist()
                sql_update = text(f"""
                    UPDATE archive.{table_name}
                    SET end_date = :now
                    WHERE {key_column} = :key
                    AND end_date = '9999-12-31'
                """)
                for key in keys_to_close:
                    conn.execute(sql_update, {"now": now, "key": key})

        # 7. Ubaci nove redove
        new_versions.to_sql(table_name, con=engine, schema="archive", if_exists="append", index=False)
        print(f"✅ Ubaceno {len(new_versions)} novih redova u archive.{table_name}")
