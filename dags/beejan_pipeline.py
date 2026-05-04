from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.notifications import send_failure_email, send_success_email

default_args = {
    'owner': 'Chelsea',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='beejan_dbt_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 5, 2),
    schedule='5 16 * * *',
    catchup=True,
) as dag:

    check_data = BashOperator(
        task_id='check_raw_data_exists',
        bash_command='echo "Checking raw data availability..."'
    )

    ingestion = BashOperator(
        task_id='airbyte_sync',
        bash_command="""
        curl -X POST http://host.docker.internal:8000/api/v1/connections/sync \
        -H "Content-Type: application/json" \
        -d '{"connectionId": "183783f9-e5cd-4ebe-aa78-7a666535f968"}'
        """
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command="""
        dbt run \
        --project-dir /opt/airflow/dags/beejanrides_analytics \
        --profiles-dir /opt/airflow/dags/beejanrides_analytics/.dbt
        """
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command="""
        dbt test \
        --project-dir /opt/airflow/dags/beejanrides_analytics \
        --profiles-dir /opt/airflow/dags/beejanrides_analytics/.dbt
        """
    )

    notify_success = PythonOperator(
        task_id='notify_success',
        python_callable=send_success_email,
        trigger_rule='all_success'
    )

    notify_failure = PythonOperator(
        task_id='notify_failure',
        python_callable=send_failure_email,
        trigger_rule='one_failed'
    )

    # Flow
    check_data >> ingestion >> dbt_run >> dbt_test 

    [check_data, ingestion, dbt_run, dbt_test] >> notify_failure
    [check_data, ingestion, dbt_run, dbt_test] >> notify_success