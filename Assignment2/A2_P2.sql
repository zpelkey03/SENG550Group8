DROP VIEW IF EXISTS as_of_orders;

CREATE VIEW as_of_orders AS
SELECT
    fo.order_id,
    fo.order_date,
    fo.customer_id,
    dc.city AS customer_city,
    fo.product_id,
    dp.price AS product_price,
    fo.amount
FROM fact_orders fo
JOIN dim_customers dc
    ON fo.customer_id = dc.customer_id
    AND fo.order_date >= dc.valid_from
    AND (fo.order_date < dc.valid_to OR dc.valid_to IS NULL)
JOIN dim_products dp
    ON fo.product_id = dp.product_id
    AND fo.order_date >= dp.valid_from
    AND (fo.order_date < dp.valid_to OR dp.valid_to IS NULL)
ORDER BY fo.order_id ASC;

SELECT * FROM as_of_orders;