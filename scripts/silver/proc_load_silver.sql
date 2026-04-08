/*
=============================================================
STORED PROCEDURE: silver.load_silver
=============================================================
Purpose:
    - Load data from Bronze layer to Silver layer
    - Perform data cleaning, transformation, and standardization
    - Ensure only latest and valid records are stored

Features:
    - Data cleansing (TRIM, CASE, NULL handling)
    - Date conversions (INT → DATE)
    - Deduplication using ROW_NUMBER()
    - Data standardization (gender, marital status, country)
    - Error handling using TRY-CATCH
    - Load performance tracking using timestamps

Layer:
    Bronze → Silver

Author: Tharun Kumar
Date: 2026-04-08
=============================================================
*/
-- total 6 tables 
---EXEC silver.load_silver
-->>>1)silver.crm_cust_info
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE  @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_END_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT ' ==========================================';
		PRINT' LOADING SILVER LAYER';
		
		PRINT ' ==========================================';

		PRINT'--------------------------------------------';
		PRINT'Loading CRM Tables';
		PRINT'--------------------------------------------';
		--loadind silver.crm_cust_info()
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table:silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_material_status,
			cst_gndr,     
			cst_create_date )

		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
			ELSE 'N/A'
		END cst_material_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Fmale'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'N/A'
		END 
		cst_gndr,
		cst_create_date
		FROM(
			SELECT 
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)t  WHERE  flag_last = 1 

		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR )+ 'seconds';
		--->>>2) silver.crm_prd_info
		--- loading silver.crm_prd_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table:silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting data Into:silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)  
		SELECT
		prd_id,
		--prd_key,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_' ) AS cat_id,
		SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
		prd_nm,
		ISNULL(prd_cost,0) as prd_cost,
		--prd_cost,
		--prd_line,
		CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			ELSE 'N/A'
		END AS prd_line,
		/* SHORT FORM --->
		CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN'T' THEN 'Toure' 
				ELSE 'N/A' */
		CAST(prd_start_dt AS DATE) AS prd_start_dt, 
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_end_dt)-1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info
		--WHERE SUBSTRING(prd_key,7,len(prd_key))  not IN (
		--SELECT sls_prd_key from bronze.crm_sales_details  )
		--WHERE REPLACE(SUBSTRING(prd_key,1,5),'-','_' ) NOT IN
		--(SELECT DISTINCT id from bronze.erp_px_cat_g1v2)

		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR )+ 'seconds';


		---->>3) silver.crm_sales_details
		----  loading silver.crm_sales_details

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table:silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting data Into:silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_ord_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)

		SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		--sls_ord_dt,

		TRY_CAST(
			STUFF(STUFF(CAST(sls_ord_dt AS VARCHAR(8)),5,0,'-'),8,0,'-') 
		AS DATE) AS sls_order_dt,

		TRY_CAST(
			STUFF(STUFF(CAST(sls_ship_dt AS VARCHAR(8)),5,0,'-'),8,0,'-') 
		AS DATE) AS sls_ship_dt,

		TRY_CAST(
			STUFF(STUFF(CAST(sls_due_dt AS VARCHAR(8)),5,0,'-'),8,0,'-') 
		AS DATE) AS sls_due_dt,
		--sls_sales,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_sales)
				THEN sls_quantity * ABS(sls_sales)
			ELSE sls_sales
		END AS sls_sales,

		sls_quantity,
		--sls_price
		CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / nullif(sls_quantity,0)
			ELSE sls_price
		END AS price

		FROM bronze.crm_sales_details

		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR )+ 'seconds';

		---->>4) silver.erp_cust_az12silver.erp_cust_az12
		--- loading ----
		SET @start_time = GETDATE();

		PRINT '>> Truncating Table:silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting data Into: silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12(cid,bdate,gen)
		SELECT
		CASE WHEN cid like 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
			ELSE cid
		END AS cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
			 ELSE 'n/a'
		END AS gen
		FROM bronze.erp_cust_az12
		/*WHERE CASE WHEN cid like 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
			ELSE cid
		END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info) */

		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR )+ 'seconds';

		--- clean and load to silver layer from prev (erp_loc_a101)

		---->>>5) silver.erp_loc_a101
		--- loading ---
		SET @start_time = GETDATE();

		PRINT '>> Truncating Table:silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting data Into: silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101
		(cid,cntry)
		SELECT 
		REPLACE(cid,'-','')cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry) IN('US','USA') THEN 'United States'
			 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			 ELSE TRIM(cntry)
		END AS cntry
		FROM bronze.erp_loc_a101

		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR )+ 'seconds';

		-- clean and load erp_px_cat_g1v2

		----->>>6) silver.erp_px_cat_g1v2
		----- loading ----
		SET @start_time = GETDATE();
	
		PRINT '>> Truncating Table:silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting data Into:silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2
		(id,
		cat,
		subcat,
		maintenance)
		SELECT 
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2

		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR )+ 'seconds';
		PRINT'>>>------------';

		SET @batch_end_time = GETDATE();
		PRINT ' ==========================================';
		PRINT' LOADING SILVER LAYER';
		PRINT '>>--Total Load Duration:' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR )+ 'seconds';
		PRINT ' ==========================================';

	END TRY

	BEGIN CATCH
		PRINT ' ==========================================';
		PRINT'ERROR OCCURED DURING BRONZE LAYER';
		PRINT'Error Message' + ERROR_MESSAGE();
		PRINT'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT ' =============================================';
	END CATCH
END
