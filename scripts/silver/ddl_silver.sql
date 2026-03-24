/*
===============================================================================
Stored Procedure: Load Silver Layer (Source -> Bronze) cleaned data
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'silver' schema from external CSV files. 
    It performs the following actions:
    - Truncates the silver tables before loading data.
    - derives the original data, does transformation(data standardization/normalization, data type casting, quality check, turning the bad quality data into fully reliable and cleaned data, ready for reporting purposes )

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
  -- 1. All variables must be declared here
  DECLARE @start_time DATETIME, 
            @end_time DATETIME, 
            @batch_start_time DATETIME, 
            @batch_end_time DATETIME;

  BEGIN TRY 
        SET NOCOUNT ON; 
        SET @batch_start_time = GETDATE(); -- The "Master Clock"

        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

        -------------------------------------------------------
        -- CRM TABLES
        -------------------------------------------------------

        -- 1. crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating & Inserting: silver.crm_cust_info';
        TRUNCATE TABLE [silver].[crm_cust_info];
        INSERT INTO [silver].[crm_cust_info]
    ([cst_id], [cst_key], [cst_firstname], [cst_lastname], [cst_marital_status], [cst_gndr], [cst_create_date])
  SELECT
    [cst_id], [cst_key], TRIM([cst_firstname]), TRIM([cst_lastname]),
    CASE WHEN UPPER(TRIM([cst_marital_status])) = 'M' THEN 'Married'
                 WHEN UPPER(TRIM([cst_marital_status])) = 'S' THEN 'Single'
                 ELSE 'n/a' END,
    CASE WHEN UPPER(TRIM([cst_gndr])) = 'F' THEN 'Female'
                 WHEN UPPER(TRIM([cst_gndr])) = 'M' THEN 'Male'
                 ELSE 'n/a' END,
    [cst_create_date]
  FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_
    FROM [bronze].[crm_cust_info]
    WHERE cst_id IS NOT NULL 
        ) AS src
  WHERE flag_ = 1;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- 2. crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating & Inserting: silver.crm_prd_info';
        TRUNCATE TABLE [silver].[crm_prd_info];
        INSERT INTO [silver].[crm_prd_info]
    (prd_id, prd_key, prd_cat_id, prd_key_order, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
  SELECT
    prd_id, prd_key,
    LEFT(prd_key, 5) AS cat_id,
    SUBSTRING(prd_key, CHARINDEX('-', prd_key, CHARINDEX('-', prd_key) + 1) + 1, LEN(prd_key)),
    CASE WHEN (LEN(prd_nm) - LEN(REPLACE(prd_nm, ' ', ''))) > 1 
                 THEN REPLACE(REPLACE(REPLACE(prd_nm, ' ', '-'), '--', '-'), '--', '-')     
                 ELSE REPLACE(prd_nm, ' ', '-') END,
    ISNULL(prd_cost, 0),
    CASE UPPER(TRIM(prd_line))
                 WHEN 'T' THEN 'Touring'
                 WHEN 'M' THEN 'Mountain'
                 WHEN 'R' THEN 'Road'
                 WHEN 'S' THEN 'Other Sales'
                 ELSE 'n/a' END,
    CAST(prd_start_dt AS DATE),
    CAST(DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE)
  FROM bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- 3. crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> Truncating & Inserting: silver.crm_sales_details';
        TRUNCATE TABLE [silver].[crm_sales_details];
        INSERT INTO silver.crm_sales_details
    (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
  SELECT
    sls_ord_num, sls_prd_key, sls_cust_id,
    -- TRY_CAST prevents "Conversion Failed" errors on dates
    TRY_CAST(NULLIF(CAST(sls_order_dt AS VARCHAR), '0') AS DATE),
    TRY_CAST(NULLIF(CAST(sls_ship_dt AS VARCHAR), '0') AS DATE),
    TRY_CAST(NULLIF(CAST(sls_due_dt AS VARCHAR), '0') AS DATE),
    CASE WHEN ISNULL(sls_sales,0) <= 0 OR sls_sales != (sls_quantity * ABS(sls_price))
                 THEN sls_quantity * ABS(sls_price) ELSE sls_sales END,
    sls_quantity,
    CASE WHEN ISNULL(sls_price,0) <= 0 THEN sls_sales / NULLIF(sls_quantity, 0) ELSE ABS(sls_price) END
  FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -------------------------------------------------------
        -- ERP TABLES
        -------------------------------------------------------

        -- 4. erp_CUST_AZ12
        SET @start_time = GETDATE();
        PRINT '>> Truncating & Inserting: silver.erp_CUST_AZ12';
        TRUNCATE TABLE [silver].[erp_CUST_AZ12];
        INSERT INTO [silver].[erp_CUST_AZ12]
    (CID, BDATE, GEN)
  SELECT
    CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID)) ELSE CID END,
    -- TRY_CAST is the secret to stopping the script from crashing here!
    CASE WHEN TRY_CAST(BDATE AS DATE) < '1924-01-01' OR TRY_CAST(BDATE AS DATE) > GETDATE() THEN NULL 
                 ELSE TRY_CAST(BDATE AS DATE) END,
    CASE WHEN UPPER(TRIM(REPLACE(REPLACE(GEN, CHAR(13), ''), CHAR(10), ''))) IN ('F', 'FEMALE') THEN 'Female'
                 WHEN UPPER(TRIM(REPLACE(REPLACE(GEN, CHAR(13), ''), CHAR(10), ''))) IN ('M', 'MALE') THEN 'Male'
                 ELSE 'n/a' END
  FROM bronze.erp_CUST_AZ12;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- 5. erp_LOC_A101
        SET @start_time = GETDATE();
        PRINT '>> Truncating & Inserting: silver.erp_LOC_A101';
        TRUNCATE TABLE [silver].[erp_LOC_A101];
        INSERT INTO [silver].[erp_LOC_A101]
    (CID, CNTRY)
  SELECT
    REPLACE(CID, '-', ''),
    CASE WHEN CNTRY LIKE 'U%' THEN 'United States'
                 WHEN CNTRY LIKE 'G%' THEN 'Germany'
                 WHEN CNTRY LIKE 'C%' THEN 'Canada'
                 WHEN CNTRY LIKE 'F%' THEN 'France'
                 WHEN LEN(TRIM(REPLACE(REPLACE(CNTRY, CHAR(13), ''), CHAR(10), ''))) > 5 THEN CNTRY
                 ELSE 'n/a' END
  FROM [bronze].[erp_LOC_A101];
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- 6. erp_PX_CAT_G1V2
        SET @start_time = GETDATE();
        PRINT '>> Truncating & Inserting: silver.erp_PX_CAT_G1V2';
        TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
        INSERT INTO silver.erp_PX_CAT_G1V2
    (ID, CAT, SUBCAT, MAINTENANCE)
  SELECT
    REPLACE(TRIM(ID), '_', '-'),
    CAT, SUBCAT,
    CASE WHEN UPPER(TRIM(MAINTENANCE)) = 'YES' OR LEN(MAINTENANCE) = 3 THEN 'Yes'
                 WHEN UPPER(TRIM(MAINTENANCE)) = 'NO' OR LEN(MAINTENANCE) = 2 THEN 'No'
                 ELSE 'n/a' END
  FROM [bronze].[erp_PX_CAT_G1V2];
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Final Summary Print
        SET @batch_end_time = GETDATE();
        PRINT '==================================================';
        PRINT 'SILVER LAYER LOAD COMPLETE';
        PRINT 'Total load duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '==================================================';

    END TRY
    BEGIN CATCH 
        PRINT '================================================';
        PRINT 'ERROR OCCURRED DURING LOAD';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT '================================================';
    END CATCH
END;

-- now make this store procedure similar to the bronze layer 
