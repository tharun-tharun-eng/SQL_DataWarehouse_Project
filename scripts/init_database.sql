/*****************************************************************************************
 Project:      Retail Data Warehouse
 Author:       Tharun Kumar
 Script:       Database Initialization
 Description:  Initializes the Data Warehouse environment by:
               - Creating the DataWarehouse database
               - Defining schema layers (Bronze, Silver, Gold)

==========================================================================================
⚠️  WARNINGS & PRECAUTIONS
------------------------------------------------------------------------------------------
1. This script creates a NEW database.
   ➤ Ensure "DataWarehouse" does not conflict with an existing production database.

2. Do NOT run this script on a production server without approval.
   ➤ It may overwrite or interfere with existing environments.

3. Requires sufficient permissions:
   ➤ User must have CREATE DATABASE and CREATE SCHEMA privileges.

4. Safe Re-run:
   ➤ This script is idempotent (will not recreate existing objects).

5. Environment Recommendation:
   ➤ Use in Development / Testing environment before Production deployment.
==========================================================================================
******************************************************************************************/

/*****************************************************************************************
 STEP 1: Create Database (if not exists)
******************************************************************************************/
USE master;
GO

IF NOT EXISTS (
    SELECT name 
    FROM sys.databases 
    WHERE name = 'DataWarehouse'
)
BEGIN
    PRINT 'Creating database "DataWarehouse"...';
    CREATE DATABASE DataWarehouse;
    PRINT 'Database created successfully.';
END
ELSE
BEGIN
    PRINT 'Database "DataWarehouse" already exists. Skipping creation.';
END
GO

/*****************************************************************************************
 STEP 2: Switch to Target Database
******************************************************************************************/
USE DataWarehouse;
GO

/*****************************************************************************************
 STEP 3: Create Schemas (Bronze, Silver, Gold)

 Schema Purpose:
 - Bronze : Raw data ingestion layer (no transformation)
 - Silver : Cleaned and transformed data layer
 - Gold   : Aggregated, business-ready data layer

 ⚠️ WARNING:
 - Do not drop or modify schemas in production without impact analysis.
 - Schema changes may break ETL pipelines and reporting systems.
******************************************************************************************/

-- Create Bronze Schema
IF NOT EXISTS (
    SELECT * FROM sys.schemas WHERE name = 'bronze'
)
BEGIN
    PRINT 'Creating schema: bronze';
    EXEC('CREATE SCHEMA bronze');
    PRINT 'Schema "bronze" created successfully.';
END
ELSE
BEGIN
    PRINT 'Schema "bronze" already exists. Skipping.';
END
GO

-- Create Silver Schema
IF NOT EXISTS (
    SELECT * FROM sys.schemas WHERE name = 'silver'
)
BEGIN
    PRINT 'Creating schema: silver';
    EXEC('CREATE SCHEMA silver');
    PRINT 'Schema "silver" created successfully.';
END
ELSE
BEGIN
    PRINT 'Schema "silver" already exists. Skipping.';
END
GO

-- Create Gold Schema
IF NOT EXISTS (
    SELECT * FROM sys.schemas WHERE name = 'gold'
)
BEGIN
    PRINT 'Creating schema: gold';
    EXEC('CREATE SCHEMA gold');
    PRINT 'Schema "gold" created successfully.';
END
ELSE
BEGIN
    PRINT 'Schema "gold" already exists. Skipping.';
END
GO

/*****************************************************************************************
 ✅ SCRIPT EXECUTION COMPLETED

 Next Steps:
 - Create Bronze tables (raw data ingestion)
 - Build ETL pipelines (Bronze → Silver → Gold)
 - Create Fact & Dimension tables

******************************************************************************************/
