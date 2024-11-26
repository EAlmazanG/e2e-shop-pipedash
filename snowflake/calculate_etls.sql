-- Generate denormalized etls to sync with Tableau

-- set database and schema to use

USE DATABASE E2E_SHOP_PIPEDASH
;
USE SCHEMA BI
;

-- Create table bi.int_items
create or replace table bi.int_items as
    select 
        ft.item_uuid,
        di.item_description,
        di.item_family_id,
        df.item_family_description,
        df.item_category,
        di.is_operational_item,
        di.is_unknown_item,
        di.is_special_item,
        di.is_modification_item,
        di.is_error_item,
        round(sum(ft.total_price_eur), 2) as total_sales_amount,
        count(distinct ft.customer_id) as total_customers,
        sum(ft.quantity_amount) as total_quantity,
        count(distinct ft.transaction_id) as total_transactions
    from 
        bi.fact_transactions as ft
        inner join bi.dim_items as di on 
            ft.item_uuid = di.item_uuid
        inner join bi.dim_item_family as df on 
            di.item_family_id = df.item_family_id
    group by 
        ft.item_uuid, 
        di.item_description, 
        di.item_family_id, 
        df.item_family_description, 
        df.item_category, 
        di.is_operational_item, 
        di.is_unknown_item, 
        di.is_special_item, 
        di.is_modification_item, 
        di.is_error_item
        
;

-- Create table bi.etl_items
create or replace table bi.etl_items as
    select * from bi.int_items
    
;

-- Create table bi.int_transactions
create or replace table bi.int_transactions as
    select 
        ft.transaction_id,
        ft.invoice_id,
        ft.item_uuid,
        di.item_id,
        di.item_description,
        df.item_family_description,
        df.item_category,
        di.item_variant,
        di.is_operational_item,
        di.is_unknown_item,
        di.is_special_item,
        di.is_modification_item,
        di.is_error_item,
        round(ft.unit_price_eur, 2) as unit_price_eur,
        round(ft.total_price_eur, 2) as total_price_eur,
        ft.quantity_amount,
        ft.customer_id,
        dl.country_id,
        dl.country_name,
        dd.date_id,
        dd.date,
        dd.day_of_week,
        case 
            when di.is_operational_item = false 
                 and di.is_unknown_item = false 
                 and di.is_special_item = false 
                 and di.is_modification_item = false 
                 and di.is_error_item = false 
                 and (ft.quantity_amount < 0 or ft.total_price_eur < 0) 
            then true 
            else false 
        end as is_return
    from 
        bi.fact_transactions as ft
        left join bi.dim_items as di on 
            ft.item_uuid = di.item_uuid
        left join bi.dim_item_family as df on 
            di.item_family_id = df.item_family_id
        left join bi.dim_location as dl on 
            ft.country_id = dl.country_id
        left join bi.dim_date as dd on 
            ft.date_id = dd.date_id
            
;

-- Create table bi.etl_transactions
create or replace table bi.etl_transactions as
    select * from bi.int_transactions
 
;

-- Create table bi.int_invoices
create or replace table bi.int_invoices as
    select 
        it.invoice_id,
        it.date_id,
        dd.date,
        it.customer_id,
        dl.country_name,
        dd.day_of_week,
        dl.country_id,
        round(sum(it.total_price_eur), 2) as total_invoice_amount,
        sum(it.quantity_amount) as total_quantity,
        count(distinct it.item_uuid) as total_items,
        max(it.is_return) as has_return,
        max(it.is_operational_item) as has_operational_item,
        max(it.is_unknown_item) as has_unknown_item,
        max(it.is_special_item) as has_special_item,
        max(it.is_modification_item) as has_modification_item,
        max(it.is_error_item) as has_error_item
    from 
        bi.int_transactions as it 
        left join bi.dim_date as dd on 
            it.date_id = dd.date_id
        left join bi.dim_location as dl on
            it.country_id = dl.country_id
    group by 
        it.invoice_id,
        it.date_id, 
        dd.date, 
        it.customer_id, 
        dl.country_name, 
        dd.day_of_week, 
        dl.country_id
        
;

-- Create table bi.etl_invoices
create or replace table bi.etl_invoices as
    select * from bi.int_invoices

;

-- Create table bi.int_customers
create or replace table bi.int_customers as
    with 
        favorite_category_per_customer as (
            select 
                customer_id,
                item_category,
                count(*) as category_count,
                row_number() over (
                    partition by customer_id 
                    order by count(*) desc
                ) as category_rank
            from bi.int_transactions
            group by 
                customer_id, 
                item_category
        )
    
    select 
        it.customer_id,
        dl.country_name,
        round(sum(it.total_price_eur), 2) as total_sales_amount,
        sum(it.quantity_amount) as total_quantity,
        count(distinct it.transaction_id) as total_transactions,
        count(distinct it.invoice_id) as total_invoices,
        sum(case when it.is_return then 1 else 0 end) as total_returns,
        fc.item_category as favorite_category,
        round(sum(it.total_price_eur) / nullif(count(distinct it.transaction_id), 0),2) as avg_sales_per_transaction
        from 
            bi.int_transactions as it 
            left join bi.dim_location as dl on 
                it.country_id = dl.country_id
            left join favorite_category_per_customer as fc on 
                it.customer_id = fc.customer_id and fc.category_rank = 1
    group by 
        it.customer_id, 
        dl.country_name, 
        fc.item_category
        
;

-- Create table bi.etl_customers
create or replace table bi.etl_customers as
    select * from int_customers

;
