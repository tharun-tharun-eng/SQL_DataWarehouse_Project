# /*

# Quality Checks: Gold Layer

Script Purpose:
This script performs quality checks to validate the integrity,
consistency, and accuracy of the Gold Layer.

```
These checks ensure:
- Uniqueness of surrogate keys in dimension tables
- Referential integrity between fact and dimension tables
- Data consistency for analytical reporting
```

Usage Notes:
- Run after Gold layer creation
- Investigate any returned records (should be ZERO rows)

# Author: Tharun Kumar

*/

-- =========================================================
-- Checking: gold.dim_customers
-- =========================================================

-- Check for duplicate customer_key (Expectation: No results)
SELECT
customer_key,
COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- Check distinct gender values (data standardization)
SELECT DISTINCT gender
FROM gold.dim_customers;

-- =========================================================
-- Checking: gold.dim_products
-- =========================================================

-- Check for duplicate product_key (Expectation: No results)
SELECT
product_key,
COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- =========================================================
-- Checking: gold.fact_sales
-- =========================================================

-- Preview data
SELECT TOP 10 *
FROM gold.fact_sales;

-- =========================================================
-- Referential Integrity Checks
-- =========================================================

-- Check missing customers in fact table
-- Expectation: No results
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE c.customer_key IS NULL;

-- Check missing products in fact table
-- Expectation: No results
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL;

-- =========================================================
-- Data Consistency Checks
-- =========================================================

-- Validate product key uniqueness from Silver source
-- Expectation: No results
SELECT
prd_key,
COUNT(*)
FROM (
SELECT
pn.prd_key
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL
) t
GROUP BY prd_key
HAVING COUNT(*) > 1;
