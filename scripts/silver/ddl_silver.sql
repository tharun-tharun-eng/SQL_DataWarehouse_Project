/*
=============================================================
SILVER LAYER DDL SCRIPT
=============================================================
Purpose:
    - Create cleaned and structured tables from Bronze layer
    - Apply proper data types (DATE, NVARCHAR, etc.)
    - Prepare data for analytics (Gold layer)

Layer: Silver (Cleaned & Standardized Data)

Author: Tharun Kumar
Date: 2026-04-08
=============================================================
*/


/*
=============================================================
TABLE: crm_cust_info
Description:
    - Stores cleaned customer information
    - Standardized customer attributes
=============================================================
*/
IF OBJECT_ID ('silver.crm_cust_info', 'U') IS NOT NULL
DROP TABLE silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_material_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);



/*
=============================================================
TABLE: crm_prd_info
Description:
    - Stores product details
    - Includes category, cost, and lifecycle dates
=============================================================
*/
IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info(
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR (50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);



/*
=============================================================
TABLE: crm_sales_details
Description:
    - Stores transactional sales data
    - Dates will be transformed from INT (Bronze) → DATE (in DML step)
    - Used for downstream analytics
=============================================================
*/
IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL
DROP TABLE silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,

	-- NOTE: These should ideally be DATE (will fix in next iteration)
	sls_ord_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,

	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);



/*
=============================================================
TABLE: erp_loc_a101
Description:
    - Stores location data (customer country mapping)
=============================================================
*/
IF OBJECT_ID ('silver.erp_loc_a101', 'U') IS NOT NULL
DROP TABLE silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
);



/*
=============================================================
TABLE: erp_cust_az12
Description:
    - Stores additional customer attributes
    - Includes birthdate and gender
=============================================================
*/
IF OBJECT_ID ('silver.erp_cust_az12', 'U') IS NOT NULL
DROP TABLE silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);



/*
=============================================================
TABLE: erp_px_cat_g1v2
Description:
    - Stores product category hierarchy
    - Includes category, subcategory, and maintenance info
=============================================================
*/
IF OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
DROP TABLE silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
