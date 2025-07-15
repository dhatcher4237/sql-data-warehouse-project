/* 
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the ingerity, consistency,
    and accuracy of the Gold layer, These checks ensure:
    -Uniqueness of surrogate keys in dimension tables.
    -Referential ingerity between fact and dimension tables.
    -Validation of relationships in the data model for analytical purposes.

Usage Notes:
    -Run these checks after data loading Silver Layer.
    -Investigate and resolve any discrepancies found during the checks.

Important Note:
    -Rough draft. This quality check needs to be fine tuned.
==============================================================================
*/
--============================================================================
--Checking 'gold.dim_customers'
--============================================================================
--Duplicate check in Customer Dimension
SELECT cst_id, COUNT(*) FROM
(SELECT 
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_martial_status,
ci.cst_gndr,
ci.cst_create_date,
ca.b_date,
ca.gen,
la.country
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON		  ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON		  ci.cst_key = la.cid
)t GROUP BY cst_id
HAVING COUNT(*) > 1

--Data intergration
SELECT DISTINCT
	ci.cst_gndr,
	ca.gen,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr --CRM is the Master for gender Info
		 ELSE COALESCE(ca.gen, 'n/a')
	END AS new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON		  ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON		  ci.cst_key = la.cid
ORDER BY 1,2

--Check for duplicates in Product Dimension
SELECT prd_key, COUNT(*) FROM (
SELECT 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_name,
pn.prd_cost,
pn.prd_line,
pn.prd_start_date,
pc.cat,
pc.sub_cat,
pc.maintenance
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE  prd_end_date IS NULL -- Filter out all historical data
)t GROUP BY prd_key
HAVING COUNT(*) > 1


-- Foreign Key Integrity (Dimensions) Sales Facts
SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE c.customer_key IS NULL

