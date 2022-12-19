import os
import datetime as dt
import re

from libs.pg_connect import PgConnect
from libs.vertica_connect import VerticaConnect

from local_loader.repository import LocalRepository
from stg_loader.repository import StgRepository

from dotenv import load_dotenv
load_dotenv()


class AppConfig(object):

    def __init__(self) -> None:
        self.pg_source_host = str(os.getenv('PG_SOURCE_HOST'))
        self.pg_source_port = int(str(os.getenv('PG_SOURCE_PORT')))
        self.pg_source_dbname = str(os.getenv('PG_SOURCE_DBNAME'))
        self.pg_source_user = str(os.getenv('PG_SOURCE_USER'))
        self.pg_source_password = str(os.getenv('PG_SOURCE_PASSWORD'))

        self.vertica_warehouse_host = str(os.getenv('VERTICA_WAREHOUSE_HOST'))
        self.vertica_warehouse_port = int(str(os.getenv('VERTICA_WAREHOUSE_PORT')))
        self.vertica_warehouse_dbname = str(os.getenv('VERTICA_WAREHOUSE_DBNAME'))
        self.vertica_warehouse_user = str(os.getenv('VERTICA_WAREHOUSE_USER'))
        self.vertica_warehouse_password = str(os.getenv('VERTICA_WAREHOUSE_PASSWORD'))
        self.vertica_warehouse_autocommit = bool(int(os.getenv('VERTICA_WAREHOUSE_AUTOCOMMIT')))

        self.STAGE_DIR = os.getenv('STG_DIR') or "."
        self.DAG_DIR = "/lessons/dags"
        self.BUSINESS_DT = '{{ ds }}'

        if re.match(self.BUSINESS_DT, "\d\d\d\d-\d\d-\d\d"):
            self.BUSINESS_DT = dt.datetime.strptime(self.BUSINESS_DT, "%Y-%m-%d")

        if not re.match(self.BUSINESS_DT, "\d\d\d\d-\d\d-\d\d"):
            self.BUSINESS_DT = dt.datetime.now().date()

    def pg_source_db(self):
        return PgConnect(
            self.pg_source_host,
            self.pg_source_port,
            self.pg_source_dbname,
            self.pg_source_user,
            self.pg_source_password
        )

    def vertica_warehouse_db(self):
        return VerticaConnect(
            self.vertica_warehouse_host,
            self.vertica_warehouse_port,
            self.vertica_warehouse_dbname,
            self.vertica_warehouse_user,
            self.vertica_warehouse_password,
            self.vertica_warehouse_autocommit
        )

    def local_repository(self):
        return LocalRepository(
            self.pg_source_db(),
            self.STAGE_DIR
        )

    def stg_repository(self):
        return StgRepository(
            self.vertica_warehouse_db()
        )

    def print_date(self, date):
        print(f"BUSINESS DT: {self.BUSINESS_DT}, date: {date}")