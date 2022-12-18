from libs.vertica_connect import VerticaConnect


class StgRepository:
    def __init__(self, db: VerticaConnect) -> None:
        self._db = db
        self._batch_size = 1024

    def _test_row_exists(self, layer=None, table=None):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""SELECT count(*) FROM  {layer}.{table}""")
                res = cur.fetchone()[0]
                return res

    def _test_table_exists(self, layer=None, table=None):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""SELECT count(*) > 0 as is_exists
                                    FROM v_catalog.all_tables t
                                    WHERE t.schema_name = '{layer}'
                                    AND t.table_name = '{table}';""")
                res = cur.fetchone()[0]
                return res

    def _inserts(self, table: str, list_of_values=[]):
        with self._db.connection() as conn:
            curs = conn.cursor()
            insert_stmt = 'INSERT INTO BAD_IDEA VALUES ({},\'a\');'

            curs.execute(
                '\n'.join(
                    [insert_stmt.format(j) for j in range(3)])
            )

    def _copy(self, layer: str, table: str, file_path: str):
        if not self._test_row_exists(layer=layer, table=table):
            with self._db.connection() as conn:
                curs = conn.cursor()
                curs.execute(f"""
                                    COPY {layer}.{table}
                                    FROM LOCAL '{file_path}'
                                    DELIMITER ','
                                    """)
        else:
            self.logger.debug(f"Table: {layer}.{table} is not empty!")

    
    def _execute(self, sql_file_path: str):
        with self._db.connection() as conn:
            curs = conn.cursor()
            with open(sql_file_path) as sql_file:
                curs.execute(sql_file.read())
