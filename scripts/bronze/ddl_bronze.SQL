/*
===========================================================================
DDL Script: Create Bronze Tables
===========================================================================
Script Purpose:
This script creates tables in the 'bronze' schema, dropping existing tables
if they already exist.
Run this script to re-define the DDL structure of 'bronze' Tables
===========================================================================
*/

IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL 
	DROP TABLE bronze.crm_cust_info
CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_material_status NVARCHAR (50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);

IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL 
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_name NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR (50),
	prd_start_date DATETIME,
	prd_end_date DATETIME
);

IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL 
	DROP TABLE bronze.crm_sales_details
CREATE TABLE bronze.crm_sales_details (
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_date INT,
	sls_ship_date INT,
	sls_due_date INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);

IF OBJECT_ID ('bronze.erp_cust_az', 'U') IS NOT NULL 
	DROP TABLE bronze.erp_cust_az
CREATE TABLE bronze.erp_cust_az (
	cid NVARCHAR(50),
	b_date DATE,
	gen NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_loc_a', 'U') IS NOT NULL 
	DROP TABLE bronze.erp_loc_a
CREATE TABLE bronze.erp_loc_a (
	cid NVARCHAR(50),
	country NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_px_cat', 'U') IS NOT NULL 
	DROP TABLE bronze.erp_px_cat
CREATE TABLE bronze.erp_px_cat (
	id NVARCHAR(50),
	cat NVARCHAR(50),
	sub_cat NVARCHAR(50),
	maintenance NVARCHAR(50)
);
