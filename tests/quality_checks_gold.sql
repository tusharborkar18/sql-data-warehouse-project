/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results 
select
	customer_key,
    count(*)
from gold_dim_customers
group by customer_key
having count(*)>1;

-- ====================================================================
-- Checking 'gold.product_key'
-- ====================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 

select
	product_key,
	count(*) as duplicate_count
from gold_dim_products
group by product_key
having count(*)>1;

-- ====================================================================
-- Checking 'gold_fact_sales'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions
select
 * 
 from gold_fact_sales as f
left join gold_dim_customers as c
on f.customer_key=c.customer_key
left join gold_dim_products as p
on f.product_key=p.product_key
where p.product_key is null or c.customer_key is null;
