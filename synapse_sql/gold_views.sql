-- =============================================================================
-- Business views over the Gold star schema — the surface Power BI queries.
-- Keep BI-facing renames + calculated columns here (not in Delta) so the
-- schema stays stable across pipeline reruns.
-- =============================================================================
USE pipelinex_gold;
GO

-- Monthly revenue by product category
CREATE OR ALTER VIEW vw_monthly_revenue_by_category AS
SELECT
    d.year,
    d.month,
    d.month_name,
    p.product_category_en,
    COUNT(DISTINCT f.order_id)          AS orders,
    SUM(f.gross_revenue)                AS gross_revenue,
    SUM(f.gross_revenue * p.margin_pct) AS estimated_margin
FROM v_fact_sales   f
JOIN v_dim_date     d ON d.date_key   = f.order_date
JOIN v_dim_product  p ON p.product_id = f.product_id
WHERE p.is_current = 1
GROUP BY d.year, d.month, d.month_name, p.product_category_en;
GO

-- Customer lifetime value
CREATE OR ALTER VIEW vw_customer_ltv AS
SELECT
    c.customer_id,
    c.customer_state,
    COUNT(DISTINCT f.order_id) AS lifetime_orders,
    SUM(f.gross_revenue)       AS lifetime_revenue,
    AVG(f.review_score * 1.0)  AS avg_review_score,
    MIN(f.order_date)          AS first_order_date,
    MAX(f.order_date)          AS last_order_date
FROM v_fact_sales    f
JOIN v_dim_customer  c ON c.customer_id = f.customer_id
GROUP BY c.customer_id, c.customer_state;
GO

-- Seller performance
CREATE OR ALTER VIEW vw_seller_performance AS
SELECT
    s.seller_id,
    s.seller_state,
    COUNT(*)                        AS items_sold,
    SUM(f.gross_revenue)            AS gross_revenue,
    AVG(f.delivery_days * 1.0)      AS avg_delivery_days,
    AVG(f.review_score * 1.0)       AS avg_review_score
FROM v_fact_sales  f
JOIN v_dim_seller  s ON s.seller_id = f.seller_id
WHERE f.delivery_days IS NOT NULL
GROUP BY s.seller_id, s.seller_state;
GO

-- Weekly cohort — new vs returning
CREATE OR ALTER VIEW vw_weekly_cohort AS
WITH first_order AS (
    SELECT customer_id, MIN(order_date) AS first_date
    FROM v_fact_sales GROUP BY customer_id
)
SELECT
    DATEPART(iso_week, f.order_date) AS iso_week,
    d.year,
    COUNT(DISTINCT CASE WHEN f.order_date = fo.first_date THEN f.customer_id END) AS new_customers,
    COUNT(DISTINCT CASE WHEN f.order_date > fo.first_date THEN f.customer_id END) AS returning_customers,
    SUM(f.gross_revenue) AS weekly_revenue
FROM v_fact_sales  f
JOIN first_order   fo ON fo.customer_id = f.customer_id
JOIN v_dim_date    d  ON d.date_key      = f.order_date
GROUP BY DATEPART(iso_week, f.order_date), d.year;
GO
