insert into mart.f_customer_retention 
(new_customers_count,returning_customers_count,refunded_customer_count,period_name,
period_id,item_id,new_customers_revenue,returning_customers_revenue,customers_refunded)
with new_customers_cte 
as 
(
select date_id, count(customer_id) as new_customers_count, sum(payment_amount) as new_customers_revenue
from (
	select date_id, customer_id, sum(payment_amount) as payment_amount 
	from mart.f_sales fs2
	group by 1,2
	having count(id) = 1) t
group by 1
),
returning_customers_cte
AS
(
select date_id, count(customer_id) as returning_customers_count, sum(payment_amount) as returning_customers_revenue
from (
	select date_id, customer_id, sum(payment_amount) as payment_amount 
	from mart.f_sales fs2
	group by 1,2
	having count(id) > 1) t
group by 1
)
select distinct
	   new_customers_count,
	   returning_customers_count,
	   sum(case when status = 'refunded' then 1 else 0 end) over(partition by week_of_year) as refunded_customer_count,
	   month_name ||'_'|| day_of_week as period_name,
	   week_of_year as period_id,
	   item_id,
	   new_customers_revenue,
	   returning_customers_revenue,
	   date_actual,
	   sum(case when status = 'refunded' then 1 else 0 end) over() as customers_refunded
from mart.f_sales
left join mart.d_calendar using(date_id)
left join new_customers_cte using(date_id)
left join returning_customers_cte using(date_id)
where date_actual = '{{ds}}';