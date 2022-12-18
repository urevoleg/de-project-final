import csv
import os


from libs.pg_connect import PgConnect


class LocalRepository:
    def __init__(self, db: PgConnect, stg_dir: str) -> None:
        self._db = db
        self._stg_dir = stg_dir
        self._batch_size = 1024

    def _get_date_field_by_tablename(self, table):
        return {
            'public.transactions': 'transaction_dt',
            'public.currencies': 'date_update'
        }.get(table)

    def _read_from(self, table, date):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""SELECT * FROM {table} WHERE {self._get_date_field_by_tablename(table=table)}::date = '{date}'""")
                return cur.fetchall()

    def save_data_to_local_file(self, table, date):
        filename = os.path.abspath(os.path.join(self._stg_dir, f'{table}_{date}.csv'))
        rows = self._read_from(table=table, date=date)
        with open(filename, 'w') as f:
            writer = csv.writer(f)
            writer.writerows(rows)
        return filename

