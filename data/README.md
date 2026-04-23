# Data

The full **Olist Brazilian E-Commerce Public Dataset** (~100k orders, 9 CSVs)
is available on Kaggle:

> https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

`samples/` contains a truncated copy (first 1,000 rows of each table, minus
`olist_geolocation_dataset.csv` which is 59 MB) for local development and
CI. Point the pipeline at the full dataset in ADLS Bronze for real runs.

## Table map

| Silver table | CSV file |
| --- | --- |
| `orders` | `olist_orders_dataset.csv` |
| `order_items` | `olist_order_items_dataset.csv` |
| `customers` | `olist_customers_dataset.csv` |
| `products` | `olist_products_dataset.csv` |
| `sellers` | `olist_sellers_dataset.csv` |
| `payments` | `olist_order_payments_dataset.csv` |
| `reviews` | `olist_order_reviews_dataset.csv` |
| (translation) | `product_category_name_translation.csv` — used by MongoDB seeder |
