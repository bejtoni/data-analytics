-- 👁 CLEANED SCHEMA (VIEW-OVI)
CREATE SCHEMA IF NOT EXISTS cleaned;

CREATE OR REPLACE VIEW cleaned.customers AS
SELECT * FROM archive.customers WHERE end_date = '9999-12-31';

CREATE OR REPLACE VIEW cleaned.sellers AS
SELECT * FROM archive.sellers WHERE end_date = '9999-12-31';

CREATE OR REPLACE VIEW cleaned.products AS
SELECT * FROM archive.products WHERE end_date = '9999-12-31';

CREATE OR REPLACE VIEW cleaned.orders AS
SELECT * FROM archive.orders WHERE end_date = '9999-12-31';

CREATE OR REPLACE VIEW cleaned.order_reviews AS
SELECT * FROM archive.order_reviews WHERE end_date = '9999-12-31';

CREATE OR REPLACE VIEW cleaned.order_payments AS
SELECT * FROM archive.order_payments WHERE end_date = '9999-12-31';
