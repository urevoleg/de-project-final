from contextlib import contextmanager
from typing import Generator

import vertica_python
from vertica_python import Connection


class VerticaConnect():
    def __init__(self, host: str, port: int, db_name: str, user: str, pw: str, autocommit: bool) -> None:
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.pw = pw
        self.autocommit = autocommit

    def params(self) -> str:
        return {
        'host':self.host,
        'port' : self.port,
        'database' : self.db_name,
        'user' : self.user,
        'password' : self.pw,
        'autocommit' : self.autocommit
        }


    @contextmanager
    def connection(self) -> Generator[Connection, None, None]:
        conn = vertica_python.connect(**self.params())
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()