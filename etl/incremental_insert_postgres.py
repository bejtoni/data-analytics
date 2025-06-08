import psycopg2
from psycopg2 import sql


# Povezivanje s bazom podataka sa vašim parametrima
def execute_sql_from_file(file_path):
    conn = None
    try:
        # Kreiranje konekcije s bazom
        conn = psycopg2.connect(
            dbname="ecommerce",
            user="postgres",
            password="alen",
            host="localhost",
            port="5432"
        )
        cur = conn.cursor()

        # Čitanje SQL skripte iz fajla
        with open(file_path, 'r') as file:
            sql_script = file.read()

        # Izvršavanje SQL skripte
        cur.execute(sql.SQL(sql_script))

        # Potvrda promjena u bazi
        conn.commit()

        # Zatvaranje kursora
        cur.close()
        print("✅ SQL skripta uspješno izvršena!")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"❌ Greška pri izvršavanju SQL skripte: {error}")

    finally:
        if conn is not None:
            conn.close()
