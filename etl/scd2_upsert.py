import pandas as pd
from datetime import datetime
from sqlalchemy import text

def scd2_upsert(pg_engine, table_name, key_column, all_columns):
    now = datetime.now()

    print(f"\n🔁 START: SCD2 upsert za tabelu: {table_name}")

    # 1. Učitaj podatke iz staginga (landing)
    new_df = pd.read_sql(f"SELECT {', '.join(all_columns)} FROM ecommerce.{table_name}", pg_engine)
    print(f"📥 Učitavam nove podatke...\n✅ Kolone u new_df: {new_df.columns}\n🟢 new_df redova: {len(new_df)}")

    # Pretvori sve "NaT" u None
    for col in all_columns:
        if pd.api.types.is_datetime64_any_dtype(new_df[col]):
            new_df[col] = new_df[col].where(new_df[col].notna(), None)

    # 2. Učitaj aktivne podatke iz arhive
    old_df = pd.read_sql(f"""
        SELECT {', '.join(all_columns + ['start_date', 'end_date'])}
        FROM archive.{table_name}
        WHERE end_date = '9999-12-31'
    """, pg_engine)
    print(f"📦 Učitavam stare podatke...\n🔵 old_df redova: {len(old_df)}")

    # 3. Merge

    print(f"new_df is empty: {new_df.empty}")
    print(f"old_df is empty: {old_df.empty}")

    merged = pd.merge(new_df, old_df, on=key_column, how="outer", indicator=True, suffixes=('', '_old'))
    print(f"Columns in merged dataframe: {merged.columns}")
    merged["_merge_flag"] = merged["_merge"]
    merged.drop(columns=["_merge"], inplace=True)

    print(f"🔗 Radim merge()...\n📍 Merge rezultat: {len(merged)} redova")

    inserts, updates = [], []

    for row in merged.itertuples(index=False):
        row_dict = row._asdict()
        merge_flag = row_dict["_merge_flag"]

        if merge_flag == 'left_only':
            inserts.append(row)
        elif merge_flag == 'both':
            changed = any(
                row_dict[col] != row_dict.get(f"{col}_old")
                for col in all_columns if f"{col}_old" in row_dict
            )
            if changed:
                updates.append(row)

    print(f"➕ Novi redovi za insert: {len(inserts)}")
    print(f"✏️ Redovi sa promjenama za update: {len(updates)}")

    with pg_engine.begin() as conn:
        # a) Zatvori stare redove
        for row in updates:
            conn.execute(text(f"""
                UPDATE archive.{table_name}
                SET end_date = :now
                WHERE {key_column} = :key AND end_date = '9999-12-31'
            """), {"now": now, "key": getattr(row, key_column)})

        # b) Ubaci nove redove
        for row in inserts + updates:
            values = {col: getattr(row, col) for col in all_columns}
            values.update({
                "start_date": now,
                "end_date": pd.Timestamp("9999-12-31"),
            })
            columns = ", ".join(values.keys())
            placeholders = ", ".join([f":{k}" for k in values.keys()])
            conn.execute(text(f"""
                INSERT INTO archive.{table_name} ({columns}, start_date, end_date)
                VALUES ({placeholders}, :start_date, :end_date)
            """), values)

    print(f"✅ archive.{table_name} upsert completed.")
