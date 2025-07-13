/*
==============================================================================
Quality Checks 
==============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver' schemas. It includes checks for: 
    -Null or duplicate primary keys.
    -Unwanted spaces in string fields.
    -Invalid date ranges and orders.
    -Data consistency between related fields.

Usage Notes: 
    -Run these checks after data loading Silver Layer.
    -Investigate and resolve any discrepancies found during the checks.

Important Notes:
    These queries are a rough draft and all may not work correctly or may be incomplete
==============================================================================
*/

--==================================================================
--Checking 'silver.crm_cust_info'
--==================================================================
--Check for NULLs or Duplicates in Primary Key
--Expectation: No Results
SELECT 
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

--Check for unwanted Spaces
--Expectation: No Results
SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)

--Expectation: No Results
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

--Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

--==================================================================
--Checking 'silver.crm_prd_info'
--==================================================================
--Quality Checks
--Check For Nulls or Duplicates in Primary Key
--Expectation: No Result
SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

--Check for unwanted spaces
--Expectation: No Results
SELECT prd_name
FROM silver.crm_prd_info
WHERE prd_name != TRIM(prd_name)

--Check for NULLS or Negative Numbers
--Expectation: No Results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

--Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for Invalid Date Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_date < prd_start_date

SELECT * FROM silver.crm_prd_info

--==================================================================
--Checking 'silver.crm_sales_details'
--==================================================================
--Check for Invalid Dates
SELECT 
NULLIF(sls_due_date, 0) AS sls_due_date
FROM bronze.crm_sales_details
WHERE sls_due_date <= 0
OR LEN(sls_due_date) != 8 
OR sls_due_date > 20500101 
OR sls_due_date < 19000101

--Check for Invalid Date Orders
SELECT
*
FROM silver.crm_sales_details
WHERE sls_order_date > sls_ship_date OR sls_order_date > sls_due_date

--Check Data Consistency: Between Sales, Quantity, and Price
-->>Sales = Quantity * Price
-->>Values must not be NULL, zero, or negative.

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

SELECT * FROM silver.crm_sales_details

--==================================================================
--Checking 'silver.erp_cust_az12'
--==================================================================
--Identify Out-of-Range Dates
SELECT DISTINCT
b_date
FROM silver.erp_cust_az12
WHERE b_date < '1924-01-01' OR b_date > GETDATE()

--Data Standardization & Consistency
SELECT DISTINCT 
gen
FROM silver.erp_cust_az12

select * from silver.erp_cust_az12

--==================================================================
--Checking 'silver.erp_loc_a101'
--==================================================================
--Data Standardization & Consistency 
SELECT DISTINCT 
country 
FROM silver.erp_loc_a101
ORDER BY country
SELECT * FROM silver.erp_loc_a101

--==================================================================
--Checking 'silver.erp_px_cat_g1v2'
--==================================================================
--Check for unwanted spaces
SELECT * FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR sub_cat != TRIM(sub_cat) OR maintenance != TRIM(maintenance)

--Data Standardization & Consistency
SELECT DISTINCT 
maintenance
FROM silver.erp_px_cat_g1v2

SELECT * FROM silver.erp_px_cat_g1v2
