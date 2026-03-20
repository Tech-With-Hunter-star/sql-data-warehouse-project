/* =================================================
this is the ddl bronze script, the obj of the script
is to create the empty tables with the same naming 
conventions from our original data sets.

warning: be careful with the names, n of columns, they 
have to be exactly the same in order for the data to 
be efficiently loaded later on. 
======================================================
*/
-- now create the bronze layer 
IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL
  DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info
(
  cst_id INT ,
  cst_key NVARCHAR(50),
  cst_firstname NVARCHAR(50),
  cst_lastname NVARCHAR(50),
  cst_create_date DATE
)
IF OBJECT_ID('bronze.crm_prod_info','U') IS NOT NULL 
  DROP TABLE bronze.crm_prof_info;
CREATE TABLE bronze.crm_prd_info
(
  prd_id INT,
  prd_key NVARCHAR(50),
  prd_nm VARCHAR(50),
  prd_cost NVARCHAR(50),
  prd_line VARCHAR(50),
  prd_start_dt DATE,
  prd_end_dt DATE
)
GO
IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL 
  DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details
(
  sls_ord_num INT ,
  sls_prod_key NVARCHAR(50),
  sls_cust_id NVARCHAR(50),
  sls_order_dt VARCHAR(50),
  sls_ship_dt VARCHAR(50 ),
  sls_due_dt VARCHAR(50),
  sls_sales NVARCHAR(50),
  sls_quantity NVARCHAR(50),
  sls_price NVARCHAR(50)
)
GO
IF OBJECT_ID('bronze.erp_CUST_AZ12','U') IS NOT NULL 
  DROP TABLE bronze.erp_CUST_AZ12;
CREATE TABLE bronze.erp_CUST_AZ12
(
  CID INT ,
  BDATE DATE,
  GEN VARCHAR(50),

)
GO

IF OBJECT_ID('bronze.erp_LOC_A101','U') IS NOT NULL
  DROP TABLE bronze.erp_LOC_A101;
CREATE TABLE bronze.erp_LOC_A101
(
  CID INT ,
  CNTRY VARCHAR(50),

)

GO

IF OBJECT_ID('bronze.erp_PX_CAT_G1V2') IS NOT NULL
  DROP TABLE bronze.erp_PX_CAT_G1V2;
CREATE TABLE bronze.erp_PX_CAT_G1V2
(
  ID INT ,
  CAT VARCHAR(50),
  SUBCAT VARCHAR(50),
  MAINTENANCE VARCHAR(50)
)

-- CHECK WHETHER THE table exists before creating it 

-- this is the script to create the schemas(bronze, silver, gold) and tables(from 2 sources: crm&erp)

-- column mismatch for bronze.crm_cust_info, altering it 
ALTER TABLE [bronze].[crm_cust_info] 
ADD cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50);
GO
-- you cannot simply say in which order you want the table to be, you have to drop it first and recreate it later
-- 1. Delete the old table
DROP TABLE IF EXISTS [bronze].[crm_cust_info];
GO

-- 2. Create it again with the columns in your preferred order
CREATE TABLE bronze.crm_cust_info
(
  [cst_id] INT,
  [cst_key] NVARCHAR(50),
  [cst_firstname] NVARCHAR(50),
  [cst_lastname] NVARCHAR(50),
  [cst_marital_status] VARCHAR(50),
  -- Moved to where you want it
  [cst_gndr] VARCHAR(50),
  -- Moved to where you want it
  [cst_create_date] DATE
);
GO

DROP TABLE IF EXISTS [bronze].[crm.sales_details];

CREATE TABLE bronze.crm_sales_details
(
  sls_ord_num VARCHAR(50),
  sls_prd_key VARCHAR(50),
  sls_cust_id INT,
  sls_order_dt VARCHAR(50),
  sls_ship_dt VARCHAR(50),
  sls_due_dt VARCHAR(50),
  sls_sales NVARCHAR(50),
  sls_quantity NVARCHAR(50),
  sls_price NVARCHAR(50)

)
GO

DROP TABLE IF EXISTS [bronze].[erp_CUST_AZ12]; -- Replace with your actual table name
GO

CREATE TABLE [bronze].[erp_CUST_AZ12]
(
  [CID] VARCHAR(100),
  -- Change from INT to NVARCHAR to 'catch' the bad data
  -- Change other columns to NVARCHAR(MAX) or NVARCHAR(500) if they are also failing
  [BDATE] DATE,
  [GEN] VARCHAR(50)
);
GO

DROP TABLE IF EXISTS [bronze].[erp_LOC_A101]; -- Use your actual table name
GO

CREATE TABLE [bronze].[erp_LOC_A101]
(
  [CID] VARCHAR(50),
  -- Changed to NVARCHAR to stop the 'Type Mismatch'
  [CNTRY] VARCHAR(50)
  -- Add the rest of your columns as NVARCHAR(255)
);
GO

DROP TABLE IF EXISTS [bronze].[erp_PX_CAT_G1V2];
GO

CREATE TABLE [bronze].[erp_PX_CAT_G1V2]
(
  ID VARCHAR(50),
  CAT VARCHAR(50),
  SUBCAT VARCHAR(50),
  MAINTENANCE VARCHAR(50)

)
