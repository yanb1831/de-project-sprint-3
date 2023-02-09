create table if not exists mart.f_customer_retention (
    new_customers_count int null,
    returning_customers_count int null,
    refunded_customer_count int null,
    period_name varchar(50),
    period_id int,
    item_id int,
    new_customers_revenue numeric(10,2),
    returning_customers_revenue numeric(10,2),
    customers_refunded int,
    primary key (period_id),
    foreign key (item_id) references mart.d_item (item_id) on update cascade
);