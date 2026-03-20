/* 
===========================================================================================================
DDL script: create bronze table
===========================================================================================================
Purpose of the script: 
The script is aimed to ingest the raw data from azure database into the bronze layer. Here is what it does:
- truncates the bronze tables before loading data from our cloud platform
- Uses the `BULK INSERT` command to load data from csv Files to bronze tables.
Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;


Starting with emptying, it ends with creating the procedure of loading in the data with the Bulk in method. 
===========================================================================================================
*/

-- 1. Create the Master Key (fixed missing END here)
IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
BEGIN
    CREATE MASTER KEY ENCRYPTION BY PASSWORD='MystrongPass98';
END
GO -- Batch separator is required here

-- 2. Drop existing objects to ensure a clean slate
IF EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'MyAzureDataSource')
    DROP EXTERNAL DATA SOURCE [MyAzureDataSource];
GO

IF EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name = 'BlobStorageCredential')
    DROP DATABASE SCOPED CREDENTIAL [BlobStorageCredential];
GO

-- 3. Create the credential 
CREATE DATABASE SCOPED CREDENTIAL [BlobStorageCredential]
WITH IDENTITY = 'SHARED ACCESS SIGNATURE', 
SECRET = '?sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2026-03-20T19:42:02Z&st=2026-03-20T11:27:02Z&spr=https&sig=Bfujukir69wYE66hJpYaj0Cn%2BMrFw2MX10JopGxTPro%3D';
GO

-- 4. Create the external data source
CREATE EXTERNAL DATA SOURCE [MyAzureDataSource]
WITH (
  TYPE = BLOB_STORAGE,
  LOCATION = 'https://deltastoragehunterhome.blob.core.windows.net/datawrehousedata',
  CREDENTIAL = [BlobStorageCredential]
);
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN 
  DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME, @batch_end_time DATETIME;
-- handling the error is the very first thing to do 
    BEGIN TRY
        PRINT '===================================================';
        PRINT 'Starting Bronze Layer Load: ';
        PRINT '===================================================';
        -- 1. CRM - Customer Info
        PRINT '-------------------------';
        PRINT 'Loading: crm_cust_info...';
        PRINT '-------------------------';
        SET @start_time=GETDATE();
        SET @batch_start_time=GETDATE();
        PRINT '>> Truncating/emptying the table';
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT '>> Inserting the data.......';
        BULK INSERT bronze.crm_cust_info FROM 'cust_info.csv'
        WITH (DATA_SOURCE = 'MyAzureDataSource', 
              FIRSTROW = 2, FIELDTERMINATOR = ',',
              ROWTERMINATOR = '0x0a', 
              TABLOCK);
        PRINT '>> Data successfully loaded: ' ;-- IN ANY ETL is important to understand the duration of each step 
        SET @end_time=GETDATE();
        -- 2. CRM - Product Info
        PRINT '-------------------------';
        PRINT 'Loading: crm_prd_info...';
        PRINT '-------------------------';
        SET @start_time=GETDATE()
        TRUNCATE TABLE bronze.crm_prd_info;
        BULK INSERT bronze.crm_prd_info FROM 'prd_info.csv'
        WITH (DATA_SOURCE = 'MyAzureDataSource', 
              FIRSTROW = 2, FIELDTERMINATOR = ',',
              ROWTERMINATOR = '0x0a', 
              TABLOCK);
        PRINT '>> Data successfully loaded: ' + CAST(GETDATE() AS VARCHAR);
        SET @end_time=GETDATE()
        -- 3. CRM - Sales Details
        PRINT '-------------------------';
        PRINT 'Loading: crm_sales_details...';
        PRINT '-------------------------';
        SET @start_time=GETDATE();
        PRINT 'Truncating the table...'
        TRUNCATE TABLE bronze.crm_sales_details;
        BULK INSERT bronze.crm_sales_details FROM 'sales_details.csv'
        WITH (DATA_SOURCE = 'MyAzureDataSource', 
              FIRSTROW = 2, FIELDTERMINATOR = ',', 
              ROWTERMINATOR = '0x0a', 
              TABLOCK);
        PRINT '>> Data successfully loaded: ' + CAST(GETDATE() AS VARCHAR);
        SET @end_time=GETDATE();
        -- 4. ERP - Customer AZ12
        PRINT '-------------------------';
        PRINT 'Loading: erp_CUST_AZ12...';
        PRINT '-------------------------';
        SET @start_time=GETDATE();
        TRUNCATE TABLE bronze.erp_CUST_AZ12;
        BULK INSERT bronze.erp_CUST_AZ12 FROM 'CUST_AZ12.csv'
        WITH (DATA_SOURCE = 'MyAzureDataSource', 
              FIRSTROW = 2, FIELDTERMINATOR = ',', 
              ROWTERMINATOR = '0x0a', 
              TABLOCK);
        PRINT '>> Data successfully loaded: ' + CAST(GETDATE() AS VARCHAR);
        SET @end_time=GETDATE();
        -- 5. ERP - Location A101
        PRINT '-------------------------';
        PRINT 'Loading: erp_LOC_A101...';
        PRINT '-------------------------';
        SET @start_time=GETDATE();
        PRINT '<<<Truncating the table...'
        TRUNCATE TABLE bronze.erp_LOC_A101;
        BULK INSERT bronze.erp_LOC_A101 FROM 'LOC_A101.csv'
        WITH (DATA_SOURCE = 'MyAzureDataSource', 
              FIRSTROW = 2, FIELDTERMINATOR = ',', 
              ROWTERMINATOR = '0x0a', 
              TABLOCK);
        PRINT '>> Data successfully loaded: ' + CAST(GETDATE() AS VARCHAR);
        SET @end_time=GETDATE();
        -- 6. ERP - Product Category G1V2
        PRINT '-------------------------';
        PRINT 'Loading: erp_PX_CAT_G1V2...';
        PRINT '-------------------------';
        SET @start_time=GETDATE();
        PRINT '<<<Truncating the table...'
        TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
        BULK INSERT bronze.erp_PX_CAT_G1V2 FROM 'PX_CAT_G1V2.csv'
        WITH (DATA_SOURCE = 'MyAzureDataSource', 
              FIRSTROW = 2, FIELDTERMINATOR = ',', 
              ROWTERMINATOR = '0x0a', 
              TABLOCK);
        PRINT '>> Data successfully loaded: ' + CAST(GETDATE() AS VARCHAR);
        SET @end_time=GETDATE();
        SET @batch_end_time=GETDATE();
        PRINT 'Total duration of the batch load:'+CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR )+'seconds';
        PRINT '===================================================';
        PRINT 'SUCCESS: All Bronze tables loaded.';
        PRINT '===================================================';

    END TRY -- this is going to execute the try, if any error happends, the second section is going to be executed
    BEGIN CATCH-- if there's an error in the ETL
        PRINT '===================================================';
        PRINT 'ERROR detected during load!';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number:  ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Message'+CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '===================================================';
    END CATCH
END;
GO
EXEC [bronze].[load_bronze];

-- this is about building the ETL PIPELINE, how to engineer the whole pipeline,
/* manage the speed of the entire data, how to improve the loading speed, what if errors happen,
everything should be optimized and maintained clean, so the future maitanance and debugging will be much more easier. 
-- quality measure, a lot of stuff to make our data ETL professional
