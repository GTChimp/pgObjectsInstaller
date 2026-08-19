from psycopg2 import connect
from termcolor import colored
from maskpass import askpass

from poi_lib.models import PGConnectionConfig


class PostgresClient:
    def __init__(self, db_config: PGConnectionConfig):
        self._db_config = db_config
        self._ac_connection = None
        self._tx_connection = None

    def prompt_connection_details(self, prompts_default):
        from termcolor import cprint
        cprint(f'Enter the host of the Postgresql cluster, default host is: {self._db_config.host}',
               *prompts_default)
        self._db_config.host = input()
        cprint(f'Host is set to {self._db_config.host}', 'light_green')

        cprint(f'Enter the port of the Postgresql cluster, default port is: {self._db_config.port}',
               *prompts_default)
        self._db_config.port = input()
        cprint(f'Port is set to {self._db_config.port}', 'light_green')

        cprint(f'Enter the database name, default database is: {self._db_config.dbname}',
               *prompts_default)
        self._db_config.dbname = input()
        cprint(f'Database name is set to {self._db_config.dbname}', 'light_green')

        cprint(f'Enter the user name for db connection, default user is: {self._db_config.user}',
               *prompts_default)
        self._db_config.user = input()
        cprint(f'User is set to {self._db_config.user}', 'light_green')

        self._db_config.password = askpass(prompt=colored(f'Enter the password for db connection\n', 'blue'))

    @property
    def ac_connection(self):
        if self._ac_connection is None or self._ac_connection.closed:
            self._ac_connection = connect(**self._db_config.as_dict())
            self._ac_connection.set_session(autocommit=True)
        return self._ac_connection

    @property
    def tx_connection(self):
        if self._tx_connection is None or self._tx_connection.closed:
            self._tx_connection = connect(**self._db_config.as_dict())
            self._tx_connection.set_session(autocommit=False)
        return self._tx_connection

    def close(self):
        if self._ac_connection and not self._ac_connection.closed:
            self._ac_connection.close()
        if self._tx_connection and not self._tx_connection.closed:
            self._tx_connection.close()
