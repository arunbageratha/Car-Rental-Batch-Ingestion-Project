from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator 
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

# Default settings for all tasks in this DAG (owner, retries, etc.)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition: no schedule, manual trigger; accepts yyyymmdd param via Airflow params
dag = DAG(
    'car_rental_data_pipeline',
    default_args=default_args,
    description='Car Rental Data Pipeline',
    schedule_interval=None,
    start_date=datetime(2025, 9, 3),
    catchup=False,
    tags=['dev'],
    params={
        'execution_date': Param(default='NA', type='string', description='Execution date in yyyymmdd format'),
    }
)

# Python function to compute effective execution date (param overrides ds)
def get_execution_date(ds_nodash, **kwargs):
    execution_date = kwargs['params'].get('execution_date', 'NA')
    if execution_date == 'NA':
        execution_date = ds_nodash
    return execution_date

# Task: Resolve execution date and push to XCom
get_execution_date_task = PythonOperator(
    task_id='get_execution_date',
    python_callable=get_execution_date,
    provide_context=True,
    op_kwargs={'ds_nodash': '{{ ds_nodash }}'},
    dag=dag,
)

# Task: Close out changed current records in SCD2 dimension (end_date, is_current=false)
merge_customer_dim = SQLExecuteQueryOperator(
    task_id='merge_customer_dim',
    conn_id='snowflake_conn',
    sql="""
        MERGE INTO car_rental.PUBLIC.customer_dim AS target
        USING (
            SELECT
                $1 AS customer_id,
                $2 AS name,
                $3 AS email,
                $4 AS phone
            FROM @car_rental.PUBLIC.car_rental_data_stg/customers_{{ ti.xcom_pull(task_ids='get_execution_date') }}.csv (FILE_FORMAT => 'csv_format')
        ) AS source
        ON target.customer_id = source.customer_id AND target.is_current = TRUE
        WHEN MATCHED AND (
            target.name != source.name OR
            target.email != source.email OR
            target.phone != source.phone
        ) THEN
            UPDATE SET target.end_date = CURRENT_TIMESTAMP(), target.is_current = FALSE;
    """,
    dag=dag,
)

# Task: Insert new version rows as current (effective_date now, end_date null)
insert_customer_dim = SQLExecuteQueryOperator(
    task_id='insert_customer_dim',
    conn_id='snowflake_conn',
    sql="""
        INSERT INTO car_rental.PUBLIC.customer_dim (customer_id, name, email, phone, effective_date, end_date, is_current)
        SELECT
            $1 AS customer_id,
            $2 AS name,
            $3 AS email,
            $4 AS phone,
            CURRENT_TIMESTAMP() AS effective_date,
            NULL AS end_date,
            TRUE AS is_current
        FROM @car_rental.PUBLIC.car_rental_data_stg/customers_{{ ti.xcom_pull(task_ids='get_execution_date') }}.csv (FILE_FORMAT => 'csv_format');
    """,
    dag=dag,
)

CLUSTER_NAME = 'hadoop-dev-new'
PROJECT_ID = 'dev-sunset-468907-e9'
REGION = 'us-central1'

pyspark_job_file_path = 'gs://snowflake-projects-test-gds/car_rental_spark_job/spark_job.py'

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": pyspark_job_file_path,
        "args": ["--date={{ ti.xcom_pull(task_ids='get_execution_date') }}"],
        "jar_file_uris": [
            "gs://snowflake-projects-test-gds/snowflake_jars/spark-snowflake_2.12-2.15.0-spark_3.4.jar",
            "gs://snowflake-projects-test-gds/snowflake_jars/snowflake-jdbc-3.16.0.jar"
        ]
    }
}

# Task: Submit PySpark job to Dataproc (passes date argument from XCom)
submit_pyspark_job = DataprocSubmitJobOperator(
    task_id='submit_pyspark_job',
    job=PYSPARK_JOB,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

# Orchestration: date -> SCD2 close -> insert current -> run Spark job
get_execution_date_task >> merge_customer_dim >> insert_customer_dim >> submit_pyspark_job
