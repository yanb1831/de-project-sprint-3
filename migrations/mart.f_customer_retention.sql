create table if not exists mart.f_customer_retention (
    new_customers_count int NULL,
    returning_customers_count int NULL,
    refunded_customer_count int NULL,
    period_name varchar(50),
    period_id int,
    item_id int,
    new_customers_revenue numeric(10,2),
    returning_customers_revenue numeric(10,2),
    customers_refunded int
);

insert into mart.f_customer_retention 
(new_customers_count,returning_customers_count,refunded_customer_count,period_name,
period_id,item_id,new_customers_revenue,returning_customers_revenue,customers_refunded)
with period_table_cte 
as
(
select extract(week from date_time) as period_id, 
	   to_char(min(date_time) over(partition by extract(week from date_time)),'mon-dd') 
	   ||' & '|| 
	   to_char(max(date_time) over(partition by extract(week from date_time)),'mon-dd') as period_name,
	   uniq_id,customer_id,item_id,payment_amount,status,
	   sum(case when status = 'refunded' then 1 else 0 end) over(partition by extract(week from date_time)) as refunded_customer_count,
	   sum(case when status = 'refunded' then 1 else 0 end) over() as customers_refunded
from staging.user_order_log
),
new_customers_cte 
as 
(
	select period_id, count(customer_id) as new_customers_count, sum(payment_amount) as new_customers_revenue
	from (
		select period_id, customer_id, sum(payment_amount) as payment_amount
		from period_table_cte
		group by 1,2
		having count(uniq_id) = 1) t
		group by 1
),
returning_customers_cte
as
(
	select period_id, count(customer_id) as returning_customers_count, sum(payment_amount) as returning_customers_revenue
	from (
		select period_id, customer_id, sum(payment_amount) as payment_amount
		from period_table_cte
		group by 1,2
		having count(uniq_id) > 1) t
		group by 1
)
select distinct
	   new_customers_count,returning_customers_count,refunded_customer_count,period_name,
	   period_id,item_id,new_customers_revenue,returning_customers_revenue,customers_refunded
from period_table_cte 
left join new_customers_cte using(period_id)
left join returning_customers_cte using(period_id)
ORDER BY period_id;