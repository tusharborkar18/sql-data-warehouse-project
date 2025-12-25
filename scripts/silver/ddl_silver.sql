/*
=======================================================================
DDL Script: Create Silver Tables
=======================================================================
Script Purpose:
	This script creates table in 'silver layer', dropping existing tables
    if they already exists.
    Run this script to redefine the DDL structure of 'bronze' tables
*/

drop table if exists silver_crm_cust_info;

create table silver_crm_cust_info(
cst_id 				int,
cst_key 			nvarchar(50),
cst_firstname 		nvarchar(50),
cst_lastname 		nvarchar(50),
cst_marital_status 	nvarchar(50),
cst_gndr 			nvarchar(50),
cst_create_date 	date,
dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

drop table if exists silver_crm_prd_info;

create table silver_crm_prd_info(
prd_id 			int,
cat_id          nvarchar(50),
prd_key 		nvarchar(50),
prd_nm 			nvarchar(50),
prd_cost 		int,
prd_line 		nvarchar(50),
prd_start_dt 	date,
prd_end_dt 		date,
dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

drop table if exists silver_crm_sales_details;

create table silver_crm_sales_details(
sls_ord_num 	nvarchar(50),
sls_prd_key 	nvarchar(50),
sls_cust_id 	int,
sls_order_dt 	date,
sls_ship_dt 	date,
sls_due_dt 		date,
sls_sales 		int,
sls_quantity 	int,
sls_price 		int,
dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

drop table if exists silver_erp_cust_az12;

create table silver_erp_cust_az12(
CID 	nvarchar(50),
BDATE 	date,
GEN 	nvarchar(50),
dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

drop table if exists silver_erp_loc_a101;

create table silver_erp_loc_a101(
CID 	nvarchar(50),
CNTRY 	nvarchar(50),
dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

drop table if exists silver_erp_px_cat_g1v2;

create table silver_erp_px_cat_g1v2(
ID 				nvarchar(50),
CAT 			nvarchar(50),
SUBCAT 			nvarchar(50),
MAINTENANCE 	nvarchar(50),
dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);
