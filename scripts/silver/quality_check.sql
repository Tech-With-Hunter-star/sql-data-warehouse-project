/*
==================================================================================================
QUALITY CHECK SCRIPT
==================================================================================================
  This script acts as the automated "gatekeeper" for the Sales data pipeline, transforming 
  raw, heterogeneous Bronze data into a refined, high-integrity Silver layer. By implementing a 
  rigorous suite of validation checks, the script systematically identifies and remediates data anomalies 
  that would otherwise compromise downstream AI modeling. Key operations include the deduplication of 
  primary keys via window functions, the sanitization of non-printable "ghost" characters, and the 
  harmonization of fragmented CRM/ERP attributes into standardized formats. By enforcing strict date-range 
  validation and reconciling inconsistent string fields, this process ensures that the four core 
  categories—Accessories, Bikes, Clothing, and Components—are built upon a foundation of mathematically 
  consistent and logically sound data. It includes checks for:
  - Null or duplicates primary keys
  - Unwanted spaces in string fields
  - Data standardization and consistency
  - invalid date ranges and orders
  - Data consistency between related fields

User notes:
  - Run these checks after data loading silver layer
  - Investigate and resolve any discrepancies found during the checks 
==================================================================================================

*/


-- quality check crm_cust_info 
-- CLEAN UP THE CODE 
SELECT* FROM [bronze].[crm_cust_info];
-- 1.CHECK the primary key, if any duplicates
SELECT [cst_id],COUNT(*)
FROM [bronze].[crm_cust_info]
GROUP BY [cst_id]
HAVING COUNT(*)>1 OR [cst_id] IS NULL;
-- We can see here's also null value in primary key, so we should take that out too

-- WE USE the window function for ranking of the duplicates according to the newest cst_create_date
SELECT*
FROM(

  SELECT*,
  ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_
  FROM [bronze].[crm_cust_info]
  WHERE cst_id IS NOT NULL 
) AS crm_cust_info 
WHERE flag_=1;

-- 2. additional spaces for string values
SELECT* FROM bronze.crm_cust_info
WHERE [cst_firstname]!=TRIM(cst_firstname);
-- the result is we got 15 records 
SELECT* FROM bronze.crm_cust_info
WHERE [cst_lastname]!=TRIM(cst_lastname);
-- we got 17 records 
-- same for marital status and gndr

SELECT* FROM bronze.crm_cust_info
WHERE [cst_marital_status]!=TRIM(cst_marital_status);-- no records

SELECT* FROM bronze.crm_cust_info
WHERE [cst_gndr]!=TRIM(cst_gndr);-- no records

-- 3. we have marital status and gndr with low cardinality, check for data consistency
SELECT DISTINCT cst_marital_status
FROM [bronze].[crm_cust_info];
-- RESULTS RETURNED: S,M,NULL

SELECT DISTINCT cst_gndr 
FROM bronze.crm_cust_info;
-- RESULTS RETURNED: NULL,F,M

-- HERE WE have to check for the data consistency, so we give a full name to the variables. instead of M/S we put married, single in it- 

SELECT 
    [cst_id],
    [cst_key],
    [cst_firstname],
    [cst_lastname],
    CASE
        WHEN cst_marital_status = 'M' THEN 'Married'
        WHEN cst_marital_status = 'S' THEN 'Single'
        ELSE 'n/a' 
    END AS [cst_marital_status], -- Added END here
    CASE
        WHEN cst_gndr = 'F' THEN 'Female'
        WHEN cst_gndr = 'M' THEN 'Male' -- Changed to check cst_gndr
        ELSE 'n/a' 
    END AS [cst_gndr],           -- Added END and changed the alias name
    [cst_create_date]
FROM [bronze].[crm_cust_info];
-- no issue with cst_create_date


-- data cleaning for erp sources data

SELECT
CID,
BDATE,
GEN
FROM [bronze].[erp_CUST_AZ12]
WHERE CID LIKE'%AW00011000';

SELECT 
CASE WHEN CID LIKE'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
ELSE CID 
END AS CID,
CASE WHEN [BDATE]<'1924-01-01' OR  BDATE>GETDATE() THEN NULL
ELSE BDATE
END AS BDATE,
CASE 
WHEN LEN(GEN)=7 THEN 'Female'
WHEN LEN(GEN)=5 THEN 'Male'
WHEN LEN(GEN)=3 AND GEN LIKE 'M' THEN 'Male'
WHEN LEN(GEN)=3 AND GEN LIKE 'F' THEN 'Female'
WHEN LEN(GEN)=2 AND GEN LIKE 'M' THEN 'Male'
WHEN LEN(GEN)=2 AND GEN LIKE 'F' THEN 'Female'
ELSE 'n/a' 
END AS GEN
FROM bronze.erp_CUST_AZ12;
-- more efficient solution:
-- CASE WHEN GEN IN ('F','Female) THEN 'Female'
-- WHEN GEN IN ('M','Male') THEN 'Male'
-- ELSE n/a

-- check for outdated dates:
SELECT*
FROM [bronze].[erp_CUST_AZ12] WHERE [BDATE]<'1924-01-01' OR  BDATE>GETDATE();
-- bad data quality, clean them up with null, or only the ones extream

SELECT DISTINCT GEN
FROM [bronze].[erp_CUST_AZ12];
-- only three options: Female, Male, n/a
-- but here we have 9 options 
SELECT DISTINCT GEN, LEN(GEN) AS len_gen
FROM [bronze].[erp_CUST_AZ12]
GROUP BY GEN;


SELECT 
DISTINCT TRIM(GEN) AS GEN,
LEN(UPPER(TRIM(GEN))) AS length_
FROM [bronze].[erp_CUST_AZ12]
GROUP BY TRIM(GEN);


-- location information
SELECT 
LEN(CID) AS length_
FROM [bronze].[erp_LOC_A101]
WHERE LEN(CID)!=11;
-- we have to remove the -
-- CNTRY %U THIS IS THE LAST CHARACTER ENDING WITH U 
-- CNTRY U% THIS IS THE FIRST CHARACTER ENDING WITH U 

SELECT
REPLACE(CID,'-','') AS CID,
CNTRY AS CNTRY_OLD,
CASE WHEN LEN(CNTRY) IN (3,4,14) AND CNTRY LIKE'U%' THEN 'United States'
WHEN LEN(CNTRY) IN (3,8) AND CNTRY LIKE'G%' THEN 'Germany'
WHEN LEN(CNTRY)=15 THEN CNTRY
WHEN LEN(CNTRY)=7 AND CNTRY LIKE'C%'THEN 'Canada'
WHEN LEN(CNTRY)=7 AND CNTRY LIKE'F%'THEN 'France'
WHEN LEN(CNTRY)=10 THEN CNTRY
ELSE 'n/a'
END AS CNTRY
FROM [bronze].[erp_LOC_A101] ;


SELECT DISTINCT CNTRY,LEN(CNTRY) AS len_
FROM [bronze].[erp_LOC_A101]
GROUP BY CNTRY;

-- after each insertion just remember to check for all data quality issues again

-- last table 
SELECT
REPLACE(TRIM(ID),'_','-') AS ID,
CAT,
SUBCAT,
CASE WHEN LEN(MAINTENANCE)=4 THEN 'Yes'
ELSE MAINTENANCE
END AS MAINTENANCE
FROM [bronze].[erp_PX_CAT_G1V2];

SELECT DISTINCT CAT
FROM [bronze].[erp_PX_CAT_G1V2];


SELECT *
FROM [bronze].[erp_PX_CAT_G1V2]
WHERE [CAT]!=TRIM(CAT);

SELECT *
FROM [bronze].[erp_PX_CAT_G1V2]
WHERE [SUBCAT]!=TRIM(SUBCAT);

SELECT DISTINCT SUBCAT
FROM [bronze].[erp_PX_CAT_G1V2];

SELECT DISTINCT MAINTENANCE
FROM [bronze].[erp_PX_CAT_G1V2];
-- NO,YES,YES 
SELECT DISTINCT MAINTENANCE,LEN(MAINTENANCE) AS len_
FROM [bronze].[erp_PX_CAT_G1V2]
GROUP BY MAINTENANCE;


-- 4 distinct categories: accessories, bikes, clothing, components


