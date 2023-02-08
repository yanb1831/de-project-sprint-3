import requests
import time
import json
import pandas as pd

from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.http_hook import HttpHook

http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')

postgres_conn_id = 'postgresql_de'
nickname = 'yanb'
cohort = '10'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}

def get_report(**context):
    report_id = None
    for i in range(20):
        response = requests.get(
            f"{http_conn_id.host}/get_report?task_id={context['ti'].xcom_pull(key='generate_report')}", headers=headers
        )
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)
    if not report_id:
        raise TimeoutError()
    context['ti'].xcom_push(key='get_report',value=report_id)

def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    increment_id = ti.xcom_pull(key='get_increment')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'
    local_filename = date.replace('-', '') + '_' + filename
    response = requests.get(s3_filename)
    response.raise_for_status()
    open(f"{local_filename}", "wb").write(response.content)

    df = pd.read_csv(local_filename)
    df=df.drop('id', axis=1)
    df=df.drop_duplicates(subset=['uniq_id'])

    if 'status' not in df.columns:
        df['status'] = 'shipped'

    df.loc[df['status'].str.contains('refunded'),'payment_amount'] = -abs(df.loc[df['status'].str.contains('refunded'),'payment_amount'])
    
    postgres_hook = PostgresHook(postgres_conn_id)
    postgres_hook.run(sql='ALTER TABLE staging.user_order_log ADD COLUMN IF NOT EXISTS status varchar(30) NULL;')

    engine = postgres_hook.get_sqlalchemy_engine()
    row_count = df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)

class CustomSimpleHttpOperator(SimpleHttpOperator):
    def __init__(self,xcom_push=False,**kwargs):
        super().__init__(**kwargs)
        self.xcom_push=xcom_push

    def execute(self,context):
        if self.xcom_push:
            response = super().execute(context)
            context['ti'].xcom_push(key=self.task_id,value=response)

business_dt = '{{ ds }}'

with DAG(
    dag_id='sales_mart',
    default_args={
        'owner': 'student',
        'retries': 0
    },
    description='Provide default dag for sprint3',
    catchup=True,
    start_date=datetime.today() - timedelta(days=7),
    end_date=datetime.today() - timedelta(days=1),
) as dag:
    generate_report = CustomSimpleHttpOperator(
        task_id='generate_report',
        http_conn_id='http_conn_id',
        endpoint='/generate_report',
        method='POST',
        xcom_push=True,
        response_filter=lambda response: json.loads(response.content)['task_id'],
        log_response=True,
        headers=headers
    )
    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report
    )
    get_increment = CustomSimpleHttpOperator(
        task_id='get_increment',
        http_conn_id='http_conn_id',
        endpoint='/get_increment?report_id={{ ti.xcom_pull(key="get_report") }}&date={{ ds }}T00:00:00',
        method='GET',
        log_response=True,
        xcom_push=True,
        response_filter=lambda response: json.loads(response.content)['data']['increment_id'],
        headers=headers
    )
    upload_user_order_inc = PythonOperator(
        task_id='upload_user_order_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_order_log_inc.csv',
                   'pg_table': 'user_order_log',
                   'pg_schema': 'staging'}
    )
    update_d_item_table = PostgresOperator(
        task_id='update_d_item',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_item.sql"
    )
    update_d_customer_table = PostgresOperator(
        task_id='update_d_customer',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_customer.sql"
    )
    update_d_city_table = PostgresOperator(
        task_id='update_d_city',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_city.sql"
    )
    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales.sql",
        parameters={"date": {business_dt}}
    )
    update_f_customer_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_customer_retention.sql",
        parameters={"date": {business_dt}}
    )
    (
        generate_report 
        >> get_report 
        >> get_increment
        >> upload_user_order_inc
        >> [update_d_item_table, update_d_city_table, update_d_customer_table]
        >> update_f_sales
        >> update_f_customer_retention
    )