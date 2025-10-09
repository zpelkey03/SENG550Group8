
-- Drop Tables if they exists
DROP TABLE IF EXISTS dim_customers CASCADE;
DROP TABLE IF EXISTS dim_products CASCADE;
DROP TABLE IF EXISTS fact_orders CASCADE;

-- 1.1 A - 
CREATE TABLE dim_customers (
	id 		BIGSERIAL 	PRIMARY KEY,
	customer_id TEXT NOT NULL,
	name 	TEXT 	NOT NULL,
	email	TEXT	NOT NULL,
	city	TEXT	NOT NULL,
	valid_from 	TIMESTAMPTZ	NOT NULL DEFAULT NOW(),
	valid_to	TIMESTAMPTZ,
	is_current BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE INDEX idx_dim_customers_current
	ON dim_customers(customer_id, is_current);

-- 1.1 B --
CREATE TABLE dim_products (
	id 		BIGSERIAL 	PRIMARY KEY,
	product_id TEXT NOT NULL,
	name 	TEXT 	NOT NULL,
	category TEXT	NOT NULL,
	price	NUMERIC(10,2)	NOT NULL,
	valid_from 	TIMESTAMPTZ	NOT NULL DEFAULT NOW(),
	valid_to	TIMESTAMPTZ,
	is_current BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE INDEX idx_dim_products_current
	ON dim_products(product_id, is_current);

-- 1.1 C --
CREATE TABLE fact_orders (
	order_id TEXT PRIMARY KEY,
	customer_id TEXT NOT NULL,
	product_id TEXT NOT NULL,
	order_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	amount NUMERIC(10,2) NOT NULL
);


--P1.2 1. --
INSERT INTO dim_customers (customer_id, name, email, city, valid_from)
VALUES
	('C1', 'Alice', 'alice@icloud.com', 'New York', '2025-10-07 11:00-06'),
	('C2', 'Bob', 'bob@icloud.com', 'Boston', '2025-10-07 11:00-06');

INSERT INTO dim_products (product_id, name, category, price, valid_from)
VALUES
	('P1', 'Laptop', 'Computing Devices', 1000, '2025-10-07 11:00-06'),
	('P2', 'Phone', 'Mobile Devices', 500, '2025-10-07 11:00-06');

--P1.2 2. --
INSERT INTO fact_orders
VALUES ('O1', 'C1', 'P1', '2025-10-07 11:05-06', 1000);

--P1.2 3. --
UPDATE dim_customers
	SET valid_to='2025-10-07 11:06-06', is_current = FALSE
WHERE customer_id='C1' AND is_current=TRUE;

INSERT INTO dim_customers (customer_id, name, email, city, valid_from, is_current)
SELECT customer_id, name, email, 'Chicago', '2025-10-07 11:06-06', TRUE
FROM dim_customers
WHERE customer_id='C1'
ORDER BY id DESC
LIMIT 1;

--P1.2 4. --
UPDATE dim_products
	SET valid_to='2025-10-07 11:10-06', is_current = FALSE
WHERE product_id='P1' AND is_current=TRUE;

INSERT INTO dim_products (product_id, name, category, price, valid_from, is_current)
SELECT product_id, name, category, 900, '2025-10-07 11:10-06', TRUE
FROM dim_products
WHERE product_id='P1'
ORDER BY id DESC
LIMIT 1;

--P1.2 5. --
INSERT INTO fact_orders
VALUES ('O2', 'C1', 'P1', '2025-10-07 11:15-06', 850);

--P1.2 6. --
UPDATE dim_customers
	SET valid_to='2025-10-07 11:15-06', is_current = FALSE
WHERE customer_id='C2' AND is_current=TRUE;

INSERT INTO dim_customers (customer_id, name, email, city, valid_from, is_current)
SELECT customer_id, name, email, 'Calgary', '2025-10-07 11:15-06', TRUE
FROM dim_customers
WHERE customer_id='C2'
ORDER BY id DESC
LIMIT 1;

--P1.2 7. --
INSERT INTO fact_orders
VALUES ('O3', 'C2', 'P2', '2025-10-07 11:15-06', 500);
--P1.2 8. --
INSERT INTO fact_orders
VALUES ('O4', 'C1', 'P1', '2025-10-07 11:15-06', 900);

-- Queries to see tables
SELECT * FROM fact_orders ORDER BY order_date ASC;
-- SELECT * FROM dim_customers;
-- SELECT * FROM dim_products;

--P2.1 1 --
CREATE OR REPLACE VIEW as_of_orders AS
SELECT
	o.order_id,
	o.order_date,
	o.customer_id,
	cm.name AS customer_name,
	cm.city AS customer_city,
	o.product_id,
	dp.name AS product_name,
	dp.price AS product_price,
	o.amount
FROM fact_orders AS o
JOIN dim_customers AS cm
	ON cm.customer_id = o.customer_id
	AND o.order_date >= cm.valid_from
	AND (cm.valid_to IS NULL OR o.order_date < cm.valid_to)
JOIN dim_products as dp
	ON dp.product_id = o.product_id
	AND o.order_date >= dp.valid_from
	AND (dp.valid_to IS NULL OR o.order_date < dp.valid_to);

SELECT * FROM as_of_orders ORDER BY order_id;