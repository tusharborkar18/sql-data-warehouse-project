/*
====================================================================================================
DDL Script:Create Bronze Tables
====================================================================================================
Script Purpose:
 This script creates a table in the 'bronze layer', dropping existing tables if they already exists.
 Run this script to redefine the DDL Structure of 'bronze' tables
 ===================================================================================================
*/

drop table if exists crm_cust_info;

create table crm_cust_info(
cst_id 				int,
cst_key 			nvarchar(50),
cst_firstname 		nvarchar(50),
cst_lastname 		nvarchar(50),
cst_marital_status 	nvarchar(50),
cst_gndr 			nvarchar(50),
cst_create_date 	date
);

drop table if exists crm_prd_info;

create table crm_prd_info(
prd_id 			int,
prd_key 		nvarchar(50),
prd_nm 			nvarchar(50),
prd_cost 		int,
prd_line 		nvarchar(50),
prd_start_dt 	datetime,
prd_end_dt 		datetime
);

drop table if exists crm_sales_details;

create table crm_sales_details(
sls_ord_num 	nvarchar(50),
sls_prd_key 	nvarchar(50),
sls_cust_id 	int,
sls_order_dt 	int,
sls_ship_dt 	int,
sls_due_dt 		int,
sls_sales 		int,
sls_quantity 	int,
sls_price 		int
);

drop table if exists erp_cust_az12;

create table erp_cust_az12(
CID 	nvarchar(50),
BDATE 	date,
GEN 	nvarchar(50)
);

drop table if exists erp_loc_a101;

create table erp_loc_a101(
CID 	nvarchar(50),
CNTRY 	nvarchar(50)
);

drop table if exists erp_px_cat_g1v2;

create table erp_px_cat_g1v2(
ID 				nvarchar(50),
CAT 			nvarchar(50),
SUBCAT 			nvarchar(50),
MAINTENANCE 	nvarchar(50)
);
