import re
import os
import datetime as dt


from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.sensors.python import PythonSensor

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


from .app_config import AppConfig
config = AppConfig()

from utils.utils import get_logger
logger = get_logger(logger_name=f"{__file__[:-3]}")


args = {
    "owner": "urev",
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1)
}


with DAG(
        dag_id='final-dag',
        default_args=args,
        catchup=False,
        schedule_interval='0 0 * * *',
        start_date=dt.datetime.now() - dt.timedelta(days=7),
) as dag:

    begin = EmptyOperator(task_id=f"begin")

    print_date = PythonOperator(
        task_id='print_date',
        python_callable=config.print_date
    )

    end = EmptyOperator(task_id=f"end")

    begin >> print_date >> end


if __name__ == '__main__':
    local_repo = config.local_repository()
    date = dt.datetime(2022, 10, 1)
    table = 'public.currencies'
    filename = local_repo.save_data_to_local_file(table=table, date=date)

    #stg_repo = config.stg_repository()
    #stg_repo._copy(layer='UREVOLEGYANDEXRU__STAGING', table='transactions', file_path=filename)

