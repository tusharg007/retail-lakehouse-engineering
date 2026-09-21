CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id BIGINT PRIMARY KEY, first_name VARCHAR(100) NOT NULL, last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL, phone VARCHAR(50), city VARCHAR(100), province VARCHAR(100), country VARCHAR(100),
    created_timestamp TIMESTAMP NOT NULL, updated_timestamp TIMESTAMP NOT NULL, is_active CHAR(1) NOT NULL CHECK (is_active IN ('Y','N'))
);
CREATE TABLE IF NOT EXISTS raw.stores (
    store_id BIGINT PRIMARY KEY, store_name VARCHAR(255) NOT NULL, city VARCHAR(100), province VARCHAR(100), country VARCHAR(100),
    created_timestamp TIMESTAMP NOT NULL, updated_timestamp TIMESTAMP NOT NULL, is_active CHAR(1) NOT NULL CHECK (is_active IN ('Y','N'))
);
CREATE TABLE IF NOT EXISTS raw.products (
    product_id BIGINT PRIMARY KEY, product_name VARCHAR(255) NOT NULL, category VARCHAR(100), brand VARCHAR(100), price NUMERIC(10,2) NOT NULL CHECK (price >= 0),
    created_timestamp TIMESTAMP NOT NULL, updated_timestamp TIMESTAMP NOT NULL, is_active CHAR(1) NOT NULL CHECK (is_active IN ('Y','N'))
);
CREATE TABLE IF NOT EXISTS raw.employees (
    employee_id BIGINT PRIMARY KEY, store_id BIGINT NOT NULL REFERENCES raw.stores(store_id), first_name VARCHAR(100) NOT NULL, last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL, job_title VARCHAR(100), salary NUMERIC(10,2) NOT NULL CHECK (salary >= 0),
    created_timestamp TIMESTAMP NOT NULL, updated_timestamp TIMESTAMP NOT NULL, is_active CHAR(1) NOT NULL CHECK (is_active IN ('Y','N'))
);
CREATE TABLE IF NOT EXISTS raw.orders (
    order_id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL REFERENCES raw.customers(customer_id), store_id BIGINT NOT NULL REFERENCES raw.stores(store_id),
    order_timestamp TIMESTAMP NOT NULL, payment_method VARCHAR(50) NOT NULL, order_status VARCHAR(50) NOT NULL,
    total_amount NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0), created_timestamp TIMESTAMP NOT NULL, updated_timestamp TIMESTAMP NOT NULL,
    is_active CHAR(1) NOT NULL CHECK (is_active IN ('Y','N'))
);
CREATE TABLE IF NOT EXISTS raw.order_items (
    order_item_id BIGINT PRIMARY KEY, order_id BIGINT NOT NULL REFERENCES raw.orders(order_id), product_id BIGINT NOT NULL REFERENCES raw.products(product_id),
    quantity INT NOT NULL CHECK (quantity > 0), unit_price NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0), line_amount NUMERIC(12,2) NOT NULL CHECK (line_amount >= 0),
    created_timestamp TIMESTAMP NOT NULL, updated_timestamp TIMESTAMP NOT NULL, is_active CHAR(1) NOT NULL CHECK (is_active IN ('Y','N'))
);

DO $$
DECLARE table_name text; key_name text;
BEGIN
  FOR table_name, key_name IN SELECT * FROM (VALUES
    ('customers','customer_id'), ('stores','store_id'), ('products','product_id'),
    ('employees','employee_id'), ('orders','order_id'), ('order_items','order_item_id')
  ) AS names(table_name, key_name)
  LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS %I_change_capture ON raw.%I', table_name, table_name);
    EXECUTE format('CREATE TRIGGER %I_change_capture AFTER INSERT OR UPDATE OR DELETE ON raw.%I FOR EACH ROW EXECUTE FUNCTION control.capture_change(%L)', table_name, table_name, key_name);
  END LOOP;
END $$;
