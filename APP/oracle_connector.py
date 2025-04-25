import oracledb
from loki_handler import logger


class OracleDB:
    def __init__(self,
                 user,
                 password,
                 dsn: str = None,
                 port: int = 1521,
                 host: str = None,
                 service_name: str = None,
                 sid: str = None,
                 min_connections: int = 2,
                 max_connections: int = 5,
                 increment: int = 1,
                 mode=oracledb.AUTH_MODE_DEFAULT):
        self.user = user
        self.password = password
        self.dsn = dsn
        self.port = port
        self.host = host
        self.service_name = service_name
        self.sid = sid
        self.min = min_connections
        self.max = max_connections
        self.increment = increment
        self.pool: oracledb.ConnectionPool = None
        self.mode = mode
        self.create_pool()

    def create_pool(self):
        self.pool: oracledb.ConnectionPool = oracledb.create_pool(
            user=self.user,
            password=self.password,
            dsn=self.dsn,
            port=self.port,
            host=self.host,
            service_name=self.service_name,
            sid=self.sid,
            min=self.min,
            max=self.max,
            increment=self.increment,
            mode=self.mode
        )
        return self.pool

    def get_connection(self, max_retries=100):
        if not self.pool:
            self.create_pool()
        connection = self.pool.acquire()
        retry_number = 0
        while retry_number <= max_retries:
            try:
                connection.ping()
                return connection
            except oracledb.Error:
                retry_number += 1
                self.pool.drop(connection)
                connection = self.pool.acquire()

    def execute_query_and_fetchall(self, query, params=None, retry_number=0, max_retries=2, error_object=None, **kwargs):
        if retry_number >= max_retries:
            raise Exception(f'Ошибка выполнения запроса. Превышено количество повторных попыток. {str(error_object)}')
        try:
            with self.get_connection().cursor() as cursor:
                cursor.execute(query, params, **kwargs)
                return cursor.fetchall()
        except oracledb.Error as e:
            log_msg={
                "msg"   : "Ошибка выполнения запроса",
                "error" : str(e)
            }
            logger.error(log_msg)
            return self.execute_query_and_fetchall(query, params, retry_number + 1, max_retries, error_object=e, **kwargs)

    def execute_query_and_commit(self, query, params=None, retry_number=0, max_retries=2, error_object=None, **kwargs):

        if retry_number >= max_retries:
            raise Exception(f'Ошибка выполнения запроса. Превышено количество повторных попыток. {str(error_object)}')
        try:
            connection = self.get_connection()
            with connection.cursor() as cursor:
                cursor.execute(query, params, **kwargs)
        except oracledb.Error as e:

            log_msg={
                "msg"   : "Ошибка выполнения запроса",
                "error" : str(e)
            }
            logger.error(log_msg)
            return self.execute_query_and_commit(query, params, retry_number + 1, max_retries, error_object=e, **kwargs)

        return connection.commit()

    def execute(self, query, params=None, retry_number=0, max_retries=2, error_object=None, **kwargs):
        if retry_number >= max_retries:
            raise Exception(f'Ошибка выполнения запроса. Превышено количество повторных попыток. {str(error_object)}')
        try:
            with self.get_connection().cursor() as cursor:
                return cursor.execute(query, params, **kwargs)
        except oracledb.Error as e:
            log_msg={
                "msg"   : "Ошибка выполнения запроса",
                "error" : str(e)
            }
            logger.error(log_msg)
            return self.execute_query_and_commit(query, params, retry_number + 1, max_retries, error_object=e, **kwargs)

    def close(self, force=True):
        if self.pool:
            self.pool.close(force=force)
            self.pool = None
        else:
            raise oracledb.Error('ConnectionPool not found.')
