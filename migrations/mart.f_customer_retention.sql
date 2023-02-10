delete from mart.f_customer_retention f
where (select distinct week_of_year as period_id
	   from mart.f_sales f
	   join mart.d_calendar c using(date_id)
	   where date_actual = '{{ ds }}') = f.period_id;

insert into mart.f_customer_retention 
(new_customers_count,returning_customers_count,refunded_customer_count,period_name,
period_id,item_id,new_customers_revenue,returning_customers_revenue,customers_refunded)
with new_customers_cte 
as 
(
select period_id, count(customer_id) as new_customers_count, sum(payment_amount) as new_customers_revenue
from (
	select week_of_year as period_id, customer_id, sum(payment_amount) as payment_amount 
	from mart.f_sales
	left join mart.d_calendar using(date_id)
	group by 1,2
	having count(id) = 1) t
group by 1
),
returning_customers_cte
as
(
select period_id, count(customer_id) as returning_customers_count, sum(payment_amount) as returning_customers_revenue
from (
	select week_of_year as period_id, customer_id, sum(payment_amount) as payment_amount 
	from mart.f_sales
	left join mart.d_calendar using(date_id)
	group by 1,2
	having count(id) > 1) t
group by 1
),
refunded_customers_cte
as
(
select period_id, count(customer_id) as refunded_customer_count, count(payment_amount) as customers_refunded
from (
	select week_of_year as period_id, customer_id, payment_amount
	from mart.f_sales
	left join mart.d_calendar using(date_id)
	where status = 'refunded') t
group by 1
)
select distinct 
	   new_customers_count,
	   returning_customers_count,
	   refunded_customer_count,
	   month_name ||'_'|| week_of_year as period_name,
	   week_of_year as period_id,
	   item_id,
	   new_customers_revenue,
	   returning_customers_revenue,
	   customers_refunded
from mart.f_sales f
join mart.d_calendar c using(date_id)
join new_customers_cte n ON c.week_of_year = n.period_id
join returning_customers_cte r ON c.week_of_year = r.period_id
join refunded_customers_cte rf ON rf.period_id = r.period_id
where date_actual = '{{ ds }}'
order by period_id, item_id;