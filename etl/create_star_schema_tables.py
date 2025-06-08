import sqlalchemy as sa
from sqlalchemy import text

# 📡 MSSQL konekcija
mssql_engine = sa.create_engine(
    "mssql+pyodbc://localhost/bi_dwh?"
    "driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)

def create_all_star_schema_tables():
    with mssql_engine.connect() as conn:
        # DIM_CUSTOMER
        conn.execute(text("""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'dim_customer'
        )
        BEGIN
            CREATE TABLE dbo.dim_customer (
                customer_id VARCHAR(255) PRIMARY KEY,
                city VARCHAR(255),
                state VARCHAR(255)
            )
        END
        """))

        # DIM_PRODUCT
        conn.execute(text("""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'dim_product'
        )
        BEGIN
            CREATE TABLE dbo.dim_product (
                product_id VARCHAR(255) PRIMARY KEY,
                category VARCHAR(255),
                weight_g DECIMAL(10, 2),
                name_length DECIMAL(10, 2),
                description_length DECIMAL(10, 2)
            )
        END
        """))

        # DIM_SELLER
        conn.execute(text("""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'dim_seller'
        )
        BEGIN
            CREATE TABLE dbo.dim_seller (
                seller_id VARCHAR(255) PRIMARY KEY,
                city VARCHAR(255),
                state VARCHAR(255)
            )
        END
        """))

        # DIM_DATE
        conn.execute(text("""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'dim_date'
        )
        BEGIN
            CREATE TABLE dbo.dim_date (
                date_key DATETIME PRIMARY KEY,
                year INT,
                month INT,
                day INT,
                hour INT,
                minute INT,
                second INT,
                weekday_name VARCHAR(50)
            )
        END
        """))

        # DIM_CURRENCY
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

        # FACT_ORDER
        conn.execute(text("""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'fact_order'
        )
        BEGIN
            CREATE TABLE dbo.fact_order (
                order_id VARCHAR(50) PRIMARY KEY,
                customer_id VARCHAR(50),
                date_key DATETIME,
                seller_id VARCHAR(50),
                total_payment_value DECIMAL(10, 2),
                total_freight_value DECIMAL(10, 2),
                product_count INT,
                review_score INT,
                order_status VARCHAR(20)
            )
        END
        """))

        conn.commit()
        print("✅ Sve dimenzije i fact_order su kreirane (ili već postoje).")
