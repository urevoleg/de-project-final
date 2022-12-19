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
        dag_id='cdm-global_metrics-dag',
        default_args=args,
        catchup=True,
        schedule_interval='0 0 * * *',
        start_date=dt.datetime(2022, 10, 31),
        end_date=dt.datetime(2022, 11, 1)
) as dag:

    begin = EmptyOperator(task_id=f"begin")

    update_cdm_global_metrics = PythonOperator(
        task_id='update_cdm_global_metrics',
        python_callable=config.stg_repository()._execute,
        op_kwargs={
            'sql_file_path': os.path.join(config.DAG_DIR, 'sql/cdm/update_global_metrics.sql')
        },
        dag=dag
    )

    end = EmptyOperator(task_id=f"end")

    begin >> update_cdm_global_metrics >> end


if __name__ == '__main__':
    pass

