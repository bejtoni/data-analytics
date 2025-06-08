import requests
import csv
from datetime import datetime
import os
import glob
import psycopg2
import pandas as pd

API_KEY = "0019b157e01ff7b5f8c2fba6"

def fetch_exchange_rate(base_currency="BRL", target_currencies=["EUR", "USD", "BAM"]):
    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M")
    url = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/{base_currency}"

    response = requests.get(url)
    data = response.json()

    if data.get("result") != "success":
        print("❌ API greška:", data)
        return None

    rates = data.get("conversion_rates", {})
    os.makedirs("data/api", exist_ok=True)
    filename = f"data/api/exchange_rates_{timestamp}.csv"

    with open(filename, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["date", "base", "target", "rate"])
        for target in target_currencies:
            rate = rates.get(target)
            if rate:
                datetime_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                writer.writerow([datetime_str, base_currency, target, rate])
            else:
                print(f"⚠️ Kurs za {target} nije pronađen.")

    print(f"✅ Kursna lista snimljena: {filename}")
    return filename


def insert_exchange_rates_to_db(csv_file, conn):
    df = pd.read_csv(csv_file)
    df = df.where(pd.notnull(df), None)

    cursor = conn.cursor()
    for _, row in df.iterrows():
        sql = """
            INSERT INTO ecommerce.exchange_rates (date, base_currency, target_currency, rate)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (date, base_currency, target_currency) DO NOTHING
        """
        cursor.execute(sql, (row['date'], row['base'], row['target'], row['rate']))

    conn.commit()
    print(f"✅ Ubaceni podaci iz API fajla u bazu: {csv_file}")


def fetch_and_insert_exchange_rates():
    csv_path = fetch_exchange_rate()

    if csv_path:
        try:
            conn = psycopg2.connect(
                dbname="ecommerce",
                user="postgres",
                password="alen",
                host="localhost",
                port="5432"
            )
            insert_exchange_rates_to_db(csv_path, conn)
        except Exception as e:
            print("❌ Greška pri konekciji ili ubacivanju u bazu:", e)
        finally:
            conn.close()
