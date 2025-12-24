/*
===============================================
Load Bronze Layer (Source-> Bronze)
===============================================
Script Purpose:
	This Script loads data into the bronze layer from external csv files.
    It performs the following actions:
    -Truncates the bronze tables before loading data.
    -Uses the load data command to load data from csv files to bronze tables.
*/

select'Truncating Table:crm_cust_info' as Message;
truncate table crm_cust_info;

select'Loading data into:crm_cust_info';
load data local infile 'C:/Users/DELL/Desktop/SQL_Data_Analyst/SQL_Data_Warehouse_Project/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
into table crm_cust_info
fields terminated by ','
lines terminated by '\r\n'
ignore 1 rows;

select'Truncating Table:crm_prd_info' as Message;
truncate table crm_prd_info;

select'Loading data into:crm_prd_info';
load data local infile 'C:/Users/DELL/Desktop/SQL_Data_Analyst/SQL_Data_Warehouse_Project/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
into table crm_prd_info
fields terminated by ','
lines terminated by '\r\n'
ignore 1 rows;

select'Truncating Table:crm_sales_details' as Message;
truncate table crm_sales_details;

select'Loading data into:crm_sales_details';
load data local infile 'C:/Users/DELL/Desktop/SQL_Data_Analyst/SQL_Data_Warehouse_Project/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
into table crm_sales_details
fields terminated by ','
lines terminated by '\r\n'
ignore 1 rows;

select'Truncating Table:erp_cust_az12' as Message;
truncate table erp_cust_az12;

select'Loading data into:erp_cust_az12';
load data local infile 'C:/Users/DELL/Desktop/SQL_Data_Analyst/SQL_Data_Warehouse_Project/sql-data-warehouse-project/datasets/source_erp/cust_az12.csv'
into table erp_cust_az12
fields terminated by ','
lines terminated by '\r\n'
ignore 1 rows;

select'Truncating Table:erp_loc_a101' as Message;
truncate table erp_loc_a101;

select'Loading data into:erp_loc_a101';
load data local infile 'C:/Users/DELL/Desktop/SQL_Data_Analyst/SQL_Data_Warehouse_Project/sql-data-warehouse-project/datasets/source_erp/loc_a101.csv'
into table erp_loc_a101
fields terminated by ','
lines terminated by'\r\n'
ignore 1 rows;

select'Truncating Table:erp_px_cat_g1v2' as Message;
truncate table erp_px_cat_g1v2;

select'Loading data into:erp_px_cat_g1v2';
load data local infile 'C:/Users/DELL/Desktop/SQL_Data_Analyst/SQL_Data_Warehouse_Project/sql-data-warehouse-project/datasets/source_erp/px_cat_g1v2.csv'
into table erp_px_cat_g1v2
fields terminated by ','
lines terminated by '\r\n'
ignore 1 rows;
