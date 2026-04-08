from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys

sys.path.insert(0, '/opt/airflow/extractors')

from ccee_extractor import extrair_pld_ccee
from aneel_extractor import extrair_tarifas_aneel
from epe_extractor import extrair_migracao_mercado_livre

default_args = {
    'owner': 'gustavo',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='pipeline_mercado_livre_energia',
    default_args=default_args,
    description='Pipeline completo de dados do mercado livre de energia',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['energia', 'mercado-livre', 'dbt'],
) as dag:

    extrair_ccee = PythonOperator(
        task_id='extrair_pld_ccee',
        python_callable=extrair_pld_ccee,
    )

    extrair_aneel = PythonOperator(
        task_id='extrair_tarifas_aneel',
        python_callable=extrair_tarifas_aneel,
    )

    extrair_epe = PythonOperator(
        task_id='extrair_migracao_mercado_livre',
        python_callable=extrair_migracao_mercado_livre,
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt_energia && /home/airflow/.local/bin/dbt run --profiles-dir . >> /opt/airflow/data/dbt_output.log 2>&1',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt_energia && /home/airflow/.local/bin/dbt test --profiles-dir . >> /opt/airflow/data/dbt_output.log 2>&1',
    )
    [extrair_ccee, extrair_aneel, extrair_epe] >> dbt_run >> dbt_test
