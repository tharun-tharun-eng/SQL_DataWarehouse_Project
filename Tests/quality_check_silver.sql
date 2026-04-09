/*
=============================================================
SILVER LAYER - DATA QUALITY CHECKS
=============================================================
Purpose:
    - Validate data after loading Silver layer
    - Check for NULLs, duplicates, invalid values
    - Ensure consistency across tables

Author: Tharun Kumar
=============================================================
*/


-- =========================================================
-- 1. crm_cust_info
-- =========================================================

-- Check NULL primary keys
SELECT * 
FROM silver.crm_cust_info
WHERE cst_id IS NULL;

-- Check duplicates
SELECT cst_id, COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;

-- Check invalid gender
SELECT DISTINCT cst_gndr 
FROM silver.crm_cust_info;



-- =========================================================
-- 2. crm_prd_info
-- =========================================================

-- NULL product IDs
SELECT * 
FROM silver.crm_prd_info
WHERE prd_id IS NULL;

-- Duplicate products
SELECT prd_key, COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_key
HAVING COUNT(*) > 1;

-- Negative cost
SELECT * 
FROM silver.crm_prd_info
WHERE prd_cost < 0;



-- =========================================================
-- 3. crm_sales_details
-- =========================================================

-- NULL keys
SELECT * 
FROM silver.crm_sales_details
WHERE sls_ord_num IS NULL 
   OR sls_prd_key IS NULL 
   OR sls_cust_id IS NULL;

-- Negative or zero sales
SELECT * 
FROM silver.crm_sales_details
WHERE sls_sales <= 0;

-- Invalid quantity
SELECT * 
FROM silver.crm_sales_details
WHERE sls_quantity <= 0;

-- Price mismatch
SELECT * 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price;



-- =========================================================
-- 4. erp_cust_az12
-- =========================================================

-- Future birthdates
SELECT * 
FROM silver.erp_cust_az12
WHERE bdate > GETDATE();

-- Invalid gender values
SELECT DISTINCT gen 
FROM silver.erp_cust_az12;



-- =========================================================
-- 5. erp_loc_a101
-- =========================================================

-- NULL country
SELECT * 
FROM silver.erp_loc_a101
WHERE cntry IS NULL OR cntry = '';

-- Duplicate customer IDs
SELECT cid, COUNT(*) 
FROM silver.erp_loc_a101
GROUP BY cid
HAVING COUNT(*) > 1;



-- =========================================================
-- 6. erp_px_cat_g1v2
-- =========================================================

-- NULL category IDs
SELECT * 
FROM silver.erp_px_cat_g1v2
WHERE id IS NULL;

-- Duplicate categories
SELECT id, COUNT(*) 
FROM silver.erp_px_cat_g1v2
GROUP BY id
HAVING COUNT(*) > 1;

-- Missing category values
SELECT * 
FROM silver.erp_px_cat_g1v2
WHERE cat IS NULL OR subcat IS NULL;
-- above content is given by chatgpt-----..
--check invalid dates
SELECT
NULLIF(sls_ord_dt,0) sls_ord_dt
FROM bronze.crm_sales_details
WHERE sls_ord_dt > 20250101 or sls_ord_dt < 19000101
-- check invalid dates  for ship dates
SELECT
NULLIF(sls_ship_dt,0) sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt<=0
or len(sls_ship_dt)!=8
or sls_ship_dt > 20250101
or sls_ship_dt < 19000101

-- check invalid datesSELECT  for due dates

NULLIF(sls_due_dt,0) sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt<=0
or len(sls_due_dt)!=8
or sls_due_dt > 20250101
or sls_due_dt < 19000101

--check for invalid dates
SELECT 
*
FROM bronze.crm_sales_details
WHERE sls_ord_dt > sls_ship_dt or sls_ord_dt > sls_due_dt 

-- Check the consistancy: b/w sale, quantity and price
-- Sales = quantity * price
--> value must not be -ve,zero or null.
select distinct
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,

CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_sales)
		THEN sls_quantity * ABS(sls_sales)
	ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / nullif(sls_quantity,0)
	ELSE sls_price
END AS price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales,
sls_quantity,
sls_price

--check for invalid dates IN Silver level crm_sls
SELECT 
*
FROM silver.crm_sales_details
WHERE sls_ord_dt > sls_ship_dt or sls_ord_dt > sls_due_dt 

-- upgrading to silver level---
select distinct
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales,sls_quantity,sls_price

select * from silver.crm_sales_details

-- CHECK for unwanted Spaces
-- Expectation : No result
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname  != TRIM(cst_firstname)

SELECT cst_firstname, cst_lastname
FROM bronze.crm_cust_info
WHERE (cst_firstname + cst_lastname) = TRIM(cst_firstname) + TRIM(cst_lastname);   

---Expectation : No results
SELECT cst_key
FROM bronze.crm_cust_info
WHERE cst_key = TRIM(cst_key)


-- Data standardization  & consistancy
SELECT DISTINCT cst_gndr,
cst_material_status
FROM bronze.crm_cust_info

SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

--- check quality of data from erp file
--Identify out-of -range  bDates
SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate< '1924-01-01' OR bdate > GETDATE()
 -- data standardization and consistancy
SELECT DISTINCT
gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen -- Normalize the gender values and handle the unknown cases
FROM bronze.erp_cust_az12
--check out-of -range  bDates from silver erp
SELECT DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate< '1924-01-01' OR bdate > GETDATE()
 -- data standardization and consistancy
SELECT DISTINCT
gen
FROM silver.erp_cust_az12
-- CLEAN AND LOAD 
INSERT INTO silver.erp_loc_a101
(cid,cntry)
SELECT 
REPLACE(cid,'-','')cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry) IN('US','USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END AS cntry -- Normalize & Handle missing or blank country codes---
FROM bronze.erp_loc_a101
-- data  Standardization and consistancy
SELECT DISTINCT 
cntry
FROM silver.erp_loc_a101
ORDER BY cntry
SELECT * FROM silver.erp_loc_a101

-- check for unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) or subcat != TRIM(subcat) or maintenance != TRIM( maintenance)

-- data standardization and consistancy
SELECT DISTINCT
cat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
subcat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
maintenance
FROM bronze.erp_px_cat_g1v2

-- check for nulls or dupliactes in the primary key 
-- Expectation: no Results;
SELECT 
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) >1 OR cst_id IS NULL

SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) >1 OR prd_id IS NULL

--Check for unwanted spaces
-- Expectation: No results
SELECT
cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) 

SELECT prd_nm
FROM bronze.crm_prd_info
where prd_nm = TRIM(prd_nm)

--Data Standardization & consistancy
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT DISTINCT prd_line
FROM silver.crm_prd_info

SELECT * FROM silver.crm_cust_info
SELECT * FROM silver.crm_prd_info

-- check for Nulls or -ve numbers
--Expectation No results
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 or prd_cost IS NULL

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 or prd_cost IS NULL

-- CHECK for Invalid Date Orders
-- Expectation : No Result
-- End date must not be earlier than the start_date
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT * FROM silver.crm_prd_info
