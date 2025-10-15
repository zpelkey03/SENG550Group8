-- A2_P3.sql — Part 3 (PostgreSQL side)

-- 1
SELECT customer_id, 
COUNT(DISTINCT city) 
AS distinct_cities
FROM dim_customers
GROUP BY customer_id
ORDER BY customer_id;

-- 2
SELECT customer_city,
SUM(amount)
AS total_sales_amount
FROM as_of_orders
GROUP BY customer_city
ORDER BY customer_city;


-- 3
SELECT SUM((product_price - amount))
AS diff_between_price_and_paid
FROM as_of_orders;