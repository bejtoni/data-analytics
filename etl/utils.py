import pandas as pd


def convert_timestamp_columns(df):
    for col in df.columns:
        if df[col].dtype == object and df[col].astype(str).str.match(r"\d{4}-\d{2}-\d{2}").any():
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except Exception as e:
                print(f"⚠️ Neuspjela konverzija za kolonu: {col}")
    return df
