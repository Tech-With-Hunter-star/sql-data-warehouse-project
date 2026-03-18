/* 
=============================================================
CREATE DATABASE AND SCHEMAS 
=============================================================
Script purpose: why we are doing this
  This script creates a new database named 'bronze/solver/gold' after checking if it already exists.
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
    within the database: 'bronze', 'silver', and 'gold'.

Warning:
    Running this script will drop the entire 'DataWarehouse' database if it exists.
    All data in the database will be permanently deleted. Proceed with caution
    and ensure you have proper backups before running this script.
*/

/* this is very important for other developers in the team*/


-- 1. Drop and recreate the Bronze layer
DROP DATABASE IF EXISTS sales_bronze;
CREATE DATABASE sales_bronze;

-- 2. Drop and recreate the Silver layer
DROP DATABASE IF EXISTS sales_silver;
CREATE DATABASE sales_silver;

-- 3. Drop and recreate the Gold layer
DROP DATABASE IF EXISTS sales_gold;
CREATE DATABASE sales_gold;
