alter table staging.user_order_log add column if not exists status varchar(30) null;

update staging.user_order_log t 
set payment_amount = -abs(t2.payment_amount)
from (select uniq_id, payment_amount
	  from staging.user_order_log
	  where status = 'refunded') t2
where t.uniq_id=t2.uniq_id;