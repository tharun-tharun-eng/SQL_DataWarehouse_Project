/*
=========================================================
stored procedure: Load Bronze Layer(Source->bronze)
==============================================================
Script Purpose:
This stored procedure loads data into the 'bronze' schema from external CSV files.
It performs the following actions:
Truncates the bronze tables before loading data.
Uses the BULK INSERT command to load data from csv Files to bronze tables.
Parameters:
None.
This stored procedure doesn't accept any parameters or return any values.

Usage Example:
  EXECUTE bronze.load_bronze;
  ==============================================================
  */
  CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN 
	DECLARE @start_time DATETIME;
	DECLARE @end_time DATETIME;
	DECLARE @batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '===================================================';
		PRINT 'LOADING Bronze Layer'
		PRINT '===================================================';

		PRINT '---------------------------------------------------';
		PRINT ' Loading CRM Tables'
		PRINT '---------------------------------------------------';
		
		SET @start_time = GETDATE(); 
		PRINT'>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT'>> inserting data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\nssri\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK

		);
		SET @end_time = GETDATE(); 
		PRINT '>> Load duration:'+ CAST(datediff(second,@start_time,@end_time) AS NVARCHAR) + 'SECONDS';
		PRINT'>>------------------';

		--SELECT  top 10 * FROM bronze.crm_cust_info;
		--SELECT count(*) FROM bronze.crm_cust_info;
		--
		SET @start_time = GETDATE(); 
		PRINT'>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT'>> inserting data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\nssri\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK

		);
		SET @end_time = GETDATE(); 
		PRINT '>> Load duration:'+ CAST(datediff(second,@start_time,@end_time) AS NVARCHAR) + 'SECONDS';
		PRINT'>>------------------';

		--SELECT count(*) FROM bronze.crm_prd_info;

		--
		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT'>> inserting data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\nssri\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK

		);
		SET @end_time = GETDATE(); 
		PRINT '>> Load duration:'+ CAST(datediff(second,@start_time,@end_time) AS NVARCHAR) + 'SECONDS';
		PRINT'>>------------------';

		--SELECT COUNT(*) FROM bronze.crm_sales_details;
		---
		PRINT '---------------------------------------------------';
		PRINT ' Loading ERP Tables'
		PRINT '---------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table : bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT'>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\nssri\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE(); 
		PRINT '>> Load duration:'+ CAST(datediff(second,@start_time,@end_time) AS NVARCHAR) + 'SECONDS';
		PRINT'>>------------------';

		--select count(*) from bronze.erp_loc_a101;
		-----
		SET @start_time = GETDATE();
		PRINT'>> Truncating Table : bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT'>> Inserting Table : bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\nssri\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE(); 
		PRINT '>> Load duration:'+ CAST(datediff(second,@start_time,@end_time) AS NVARCHAR) + 'SECONDS';
		PRINT'>>------------------';
		--select count(*) from bronze.erp_cust_az12;

		---
		SET @start_time = GETDATE();
		PRINT'>> Truncating Table : bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT'>> Inserting Table : bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\nssri\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE(); 
		PRINT '>> Load duration:'+ CAST(datediff(second,@start_time,@end_time) AS NVARCHAR) + 'SECONDS';
		PRINT'>>------------------';
		SET @batch_end_time = GETDATE();
		PRINT'===================================';
		PRINT ' Loading bronze layer is completed';
		PRINT'  - Total load duration: ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) +  'Seconds';
		PRINT'============================================================';
	END TRY
	BEGIN CATCH
		PRINT '======================================';
		PRINT ' ERROR OCCURED DURING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' +  CAST (ERROR_MESSAGE() AS NVARCHAR);
		PRINT 'Error Message' +  CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '======================================';
	END CATCH
END
