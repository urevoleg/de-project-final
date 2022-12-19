import re
import os
# для импортов из utils
import sys
sys.path.append(os.path.abspath(os.path.dirname(os.path.abspath(__file__))))
import datetime as dt


from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.sensors.python import PythonSensor

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


from utils.app_config import AppConfig
config = AppConfig()

from utils.utils import get_logger
logger = get_logger(logger_name=f"{__file__[:-3]}")


BUSINESS_DT = "{{ ds }}"


args = {
    "owner": "urev",
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    "depends_on_past": True,
}


with DAG(
        dag_id='final-dag',
        default_args=args,
        catchup=True,
        schedule_interval='0 0 * * *',
        start_date=dt.datetime(2022, 10, 24),
        end_date=dt.datetime(2022, 10, 24) + dt.timedelta(days=7)
) as dag:

    begin = EmptyOperator(task_id=f"begin")

    with TaskGroup(group_id='FROM_SOURCE') as load_to_local_files:
        for table in ("public.transactions", "public.currencies"):
            load_daily_data_from_source_to_local_files = PythonOperator(
                task_id=f'load_daily_data_from_{table}_to_local_files',
                python_callable=config.local_repository().save_data_to_local_file,
                op_kwargs={
                    'table': table,
                    'date': BUSINESS_DT
                },
                dag=dag
            )

            is_file_loaded = BashOperator(
                task_id=f'is_file_{table}_loaded',
                bash_command= 'tail {0}'.format(os.path.join(config.STAGE_DIR, f"{table}_{BUSINESS_DT}.csv"))
            )

            load_daily_data_from_source_to_local_files >> is_file_loaded

    is_all_files_loaded = EmptyOperator(task_id=f"is_all_files_loaded",
                                         trigger_rule=TriggerRule.ALL_SUCCESS)

    with TaskGroup(group_id='STG') as load_to_stg:
        for table in ("public.transactions", "public.currencies"):
            load_from_local_files_to_stg = PythonOperator(
                task_id=f'load_from_local_files_to_stg_{table}',
                python_callable=config.stg_repository()._copy,
                op_kwargs={
                    'layer': 'UREVOLEGYANDEXRU__STAGING',
                    'table': table.split('.')[-1],
                    'file_path': os.path.join(config.STAGE_DIR, f"{table}_{BUSINESS_DT}.csv")
                },
                dag=dag
            )

    end = EmptyOperator(task_id=f"end")

    begin >> load_to_local_files >> is_all_files_loaded >> end


if __name__ == '__main__':
    local_repo = config.local_repository()
    date = dt.datetime(2022, 10, 1)
    table = 'public.currencies'
    #filename = local_repo.save_data_to_local_file(table=table, date=date)

    #stg_repo = config.stg_repository()
    #stg_repo._copy(layer='UREVOLEGYANDEXRU__STAGING', table='transactions', file_path=filename)

