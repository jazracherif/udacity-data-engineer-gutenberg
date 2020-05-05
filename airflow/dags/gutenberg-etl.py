from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from lib.gutenberg import (
    download_catalog,
    download_data,
    generate_catalog,
    upload_catalog_to_s3,
    upload_data_to_s3
)
from lib.emr import (
    create_and_submit_once
)

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME')
PROJECT_BASE = os.environ.get('PROJECT_BASE')

default_args = {
    'owner': 'Cherif Jazra',
    'start_date': datetime(2020, 5, 4),
    'email_on_retry': False,
    'Catchup': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past':False
}

dag = DAG('gutenberg-etl-v4',
          default_args=default_args,
          description='Load Gutenberg catalog data into AWS S3 and process with EMR',
          schedule_interval='@weekly',
          max_active_runs=1
        )

""" Create DAG Operators """ 

start_operator_op = DummyOperator(task_id='Begin_execution',  dag=dag)


download_catalog_op = PythonOperator(
        task_id='download_raw_catalog',
        dag=dag,
        python_callable=download_catalog
    )

download_data_op = PythonOperator(
        task_id='download_data',
        dag=dag,
        python_callable=download_data
    )

generate_catalog_op = PythonOperator(
        task_id='generate_catalog',
        dag=dag,
        python_callable=generate_catalog,
        op_kwargs={'limit': None}
    )

upload_catalog_op = PythonOperator(
        task_id='upload_catalog_to_s3',
        dag=dag,
        python_callable=upload_catalog_to_s3,
    )

upload_data_op = PythonOperator(
        task_id='upload_data_to_s3',
        dag=dag,
        python_callable=upload_data_to_s3,
    )

run_spark_op = PythonOperator(
        task_id='run_spark_etl',
        dag=dag,
        python_callable=create_and_submit_once,
    )


end_operator_op = DummyOperator(task_id='Stop_execution',  dag=dag)


""" Create the DAG """

start_operator_op >> download_catalog_op
start_operator_op >> download_data_op

download_catalog_op >> generate_catalog_op
download_data_op >> generate_catalog_op

generate_catalog_op >> upload_catalog_op
generate_catalog_op >> upload_data_op

upload_catalog_op >> run_spark_op 
upload_data_op >> run_spark_op

run_spark_op >> end_operator_op