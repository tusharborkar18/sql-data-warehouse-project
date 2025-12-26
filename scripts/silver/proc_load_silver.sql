/*
===========================================================================================
Stored Procedure: Load Silver Layer( Bronze -> Silver)
===========================================================================================
Script Purpose:
	This stored procedure performs the ETL(Extract,Transform,Load) process to populate 
    the 'silver' layer tables from the bronze layer.
    Actions Performed:
		- Truncate Silver tables.
        - Inserts transformed and cleaned data from Bronze into Silver tables.
Parameters:
	None.
    This stored procedure does not accept any parameters or return any values.
Usage Example:
	call load_silver();
============================================================================================
*/

delimiter $$
create procedure load_silver()
begin
	declare start_time datetime;
    declare end_time datetime;
    declare batch_start_time datetime;
    declare batch_end_time datetime;
    
    set batch_start_time=current_timestamp();
	select'==============================================================';
    select'Loading Silver Layer';
    select'==============================================================';
    
    select'==============================================================';
    select'Loading CRM Tables';
    select'==============================================================';
    
    -- Loading silver_crm_cust_info
    set start_time=current_timestamp();
	select'Truncating Table: silver_crm_cust_info';
	truncate table silver_crm_cust_info;
	select'Inserting Data Into: silver_crm_cust_info';
	insert into silver_crm_cust_info(
		cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
	)
	select
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname,							-- Removing Unwanted Spaces
	trim(cst_lastname) as cst_lastname,
	case 
		when upper(trim(cst_marital_status))='M' then 'Married'
		when upper(trim(cst_marital_status))='S' then 'Single'
		else 'n/a'
	end as cst_marital_status,										-- Map Marital Status codes to decriptive values
	case
		when upper(trim(cst_gndr))='M' then 'Male'
		when upper(trim(cst_gndr))='F' then 'Female'
		else 'n/a'
	end as cst_gndr,												-- Map Gender codes to decriptive values
	cst_create_date
	from(
	select 
	* ,
	row_number() over(partition by cst_id order by cst_create_date)as flag_last             -- Removing Duplicate Record
	from 
	crm_cust_info
	where cst_id is not null and cst_id !=0
	)t where flag_last=1;
    
    set end_time=current_timestamp();
    select concat('>> Load Duration: ',' ', cast(timestampdiff(second,start_time,end_time) as char),' ','Seconds ') as Message;

	-- Loading silver_crm_prd_info
    set start_time=current_timestamp();
	select'Truncating Table: silver_crm_prd_info';
	truncate table silver_crm_prd_info;
	select'Inserting Data Into: silver_crm_prd_info';
	insert into silver_crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
	)
	select
	 prd_id,
	 replace(substring(prd_key,1,5),'-','_')as cat_id,   -- Extract Category Id
	 substring(prd_key,7,length(prd_key))as prd_key,	 -- Extract Product kry
	 prd_nm,
	 ifnull(prd_cost,0) as prd_cost,                      -- Replace Null Values
	 case upper(trim(prd_line))
		when 'R' then 'Road'
		when 'S' then 'Other Sales'
		when 'M' then 'Mountain'
		when 'T' then 'Touring'
		else 'n/a'
	end as prd_line,                                       -- Map product line codes to decriptive values
	 cast(prd_start_dt as date)as prd_start_dt,
	 cast(date_add(lead(prd_start_dt)over(partition by prd_key order by prd_start_dt) , interval -1 day)as date) as prd_end_dt    -- Calculate end date as one day before the next start date because prd_end_dt is lesser than prd_start_dt
	 from crm_prd_info;
     
	set end_time=current_timestamp();
    select concat('>> Load Duration: ',' ', cast(timestampdiff(second,start_time,end_time) as char),' ','Seconds ') as Message;
	 
	 -- Loading silver_crm_sales_details
     set start_time=current_timestamp();
	 select'Truncating Table: silver_crm_sales_details';
	 truncate table silver_crm_sales_details;
	 select'Inserting Data Into: silver_crm_sales_details';
	insert into silver_crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)
	select
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	case 
		when sls_order_dt =0 or length(sls_order_dt)!=8 then null
		else cast(cast(sls_order_dt as char) as date)
	end as sls_order_dt,													                 -- Handling invalid date data and type-casting date
	case
		when sls_ship_dt =0 or length(sls_ship_dt) !=8 then null
		else cast(cast(sls_ship_dt as char)as date)
	end as sls_ship_dt,
	case 
		when sls_due_dt =0 or length(sls_due_dt) !=8 then null
		else cast(cast(sls_due_dt as char)as date)
	end as sls_due_dt,
	case 
		when sls_sales is null or sls_sales<=0 or sls_sales != sls_quantity* abs(sls_price)
			then sls_quantity*abs(sls_price)
		else sls_sales
	end sls_sales,																				-- Recalculate sales if origional value is missing or incorrect
	sls_quantity,
	case
		when sls_price is null or sls_price<=0 
			then sls_sales/nullif(sls_quantity,0)
		else sls_price
	end as sls_price																			-- Derive price if origional value is invalid
	from crm_sales_details;
    
    set end_time=current_timestamp();
    select concat('>> Load Duration: ',' ', cast(timestampdiff(second,start_time,end_time) as char),' ','Seconds ') as Message;
    
    select'==============================================================';
    select'Loading ERP Tables';
    select'==============================================================';
    
    -- Loading silver_erp_cust_az12
    set start_time=current_timestamp();
	select'Truncating Table : silver_erp_cust_az12';
	truncate table silver_erp_cust_az12;
	select 'Inserting Data Into: silver_erp_cust_az12';
	insert into silver_erp_cust_az12(
		cid,
		bdate,
		gen
	)
	select 
	case 
		when cid like 'NAS%' then substring(cid,4,length(cid))
		else cid
	end as cid,														-- Remove 'NAS' prefix if present
	case 
		when bdate>current_date() then null
		else bdate
	end as bdate,													-- set future birthdates to null
	case 
		when upper(trim(gen)) in ('M','Male') then 'Male'
		when upper(trim(gen)) in ('F','Female')then 'Female'
		else 'n/a'
	end as gen														-- Normalize gender values and handle unknown cases
	from erp_cust_az12;
    
    set end_time=current_timestamp();
    select concat('>> Load Duration: ',' ', cast(timestampdiff(second,start_time,end_time) as char),' ','Seconds ') as Message;

    -- Loading silver_erp_loc_a101
    set start_time=current_timestamp();
	select'Truncating Table : silver_erp_loc_a101 ';
	truncate table silver_erp_loc_a101;
	select'Inserting Data Into: silver_erp_loc_a101';
	insert into silver_erp_loc_a101(cid,cntry)
	select 
	replace(cid,'-','')as cid,									-- Handle Invalid values
	case
		when trim(cntry)='DE' then 'Germany'
		when trim(cntry) in('US','USA')then 'United States'
		when trim(cntry) is null or trim(cntry)='' then 'n/a'
		else trim(cntry)
	end as cntry 												-- Normalize and Handle Missing  or blank country codes
	from erp_loc_a101;
    
    set end_time=current_timestamp();
    select concat('>> Load Duration: ',' ', cast(timestampdiff(second,start_time,end_time) as char),' ','Seconds ') as Message;

    -- Loading silver_erp_px_cat_g1v2
    set start_time=current_timestamp();
	select'Truncating Table: silver_erp_px_cat_g1v2';
	truncate table silver_erp_px_cat_g1v2;
	select'Inserting Data Into: silver_erp_px_cat_g1v2';
	insert into silver_erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
	)
	select
	id,
	cat,
	subcat,
	maintenance
	from 
	erp_px_cat_g1v2;
    
    set end_time=current_timestamp();
    select concat('>> Load Duration: ',' ', cast(timestampdiff(second,start_time,end_time) as char),' ','Seconds ') as Message;
    
    set batch_end_time=current_timestamp();
    select concat('>> Total Batch Load Duration: ',' ', cast(timestampdiff(second,batch_start_time,batch_end_time) as char),' ','Seconds ') as Message;
end $$
delimiter ;

call load_silver();
