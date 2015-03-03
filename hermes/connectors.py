from contextlib import closing
import psycopg2
from psycopg2.extras import DictCursor


class PostgresConnector(object):
    """
    A connector object which provides access to both a Postgres connection and
    cursor.

    Calling object should call 'disconnect' once their task is complete.
    """

    def __init__(self, dsn):
        """
        :param dsn: a DSN dictionary of the Postgres server to connect to.
                    e.g. {
                        'database': 'example_db',
                        'host': '127.0.0.1',
                        'port': 5432,
                        'user': 'example',
                        'password': 'example'
                    }
        """
        self._dsn = dsn
        self._pg_conn = None
        self._pg_cursor = None

    @property
    def pg_connection(self):
        """
        Connects to the Postgres host using the specified DSN. Automatically
        set all connections isolation level to AUTOCOMMIT.

        :return: a psycopg2 connection object
        """
        if self._pg_conn is None or self._pg_conn.closed:
            self._pg_conn = psycopg2.connect(cursor_factory=DictCursor,
                                             **self._dsn)
            self._pg_conn.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
            )
        return self._pg_conn

    @property
    def pg_cursor(self):
        """
        Opens a postgres cursor if it doesn't exist or is closed. Otherwise
        returns the current cursor

        :return: an open Postgres cursor
        """
        if self._pg_cursor is None or self._pg_cursor.closed:
            self._pg_cursor = self.pg_connection.cursor()
        return self._pg_cursor

    def disconnect(self):
        """
        Disconnects from the Postgres instance unless it is already
        disconnected.
        """
        if self._pg_conn and not self._pg_conn.closed:
            self._pg_conn.close()

    def is_server_master(self):
        """
        Checks the server referenced by the DSN to see if it is Master or
        Slave. This function will close down connections to ensure nothing
        is left behind.

        :return: a boolean indicating whether the server is master.
        """
        with closing(self.pg_connection) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.execute('SELECT pg_is_in_recovery();')
                return not cursor.fetchone()[0]
