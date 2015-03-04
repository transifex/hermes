from contextlib import closing

import psycopg2
from psycopg2.extras import DictCursor


class PostgresConnector(object):
    """
    Postgres-talking connection wrapper. A thin wrapper to encapsulate the
    complexity of creating, re-creating, and disconnecting from a Postgres
    database.
    """

    def __init__(self, dsn, cursor_factory=DictCursor):
        """
        Creating a PostgresConnector is done like so::

            from psycopg2.extras import DictCursor

            # Define a Postgres DSN dictionary
            dsn = {'database': 'example_db',
                   'host': '127.0.0.1',
                   'port': 5432,
                   'user': 'example',
                   'password': 'example'}

            cursor_factory = DictCursor

            # Pass the DSN to the PostgresConnector's constructor
            connector = PostgresConnector(dsn, cursor_factory=cursor_factory)

        :param dsn: a Postgres-compatible DSN dictionary
        :param cursor_factory: a callable :class:`~psycopg2.extensions.cursor`
            subclass
        """
        self._dsn = dsn
        self._pg_conn = None
        self._pg_cursor = None
        self._cursor_factory = cursor_factory

    @property
    def pg_connection(self):
        """
        Connects to the Postgres host, if a connection does not exist or is
        closed, using the the DSN provided in the constructor.

        Automatically sets connection isolation level to `AUTOCOMMIT
        <http://www.postgresql.org/docs/current/static
        /ecpg-sql-set-autocommit.html>`_.

        :return: a :class:`~psycopg2.extensions.connection` object
        """
        if self._pg_conn is None or self._pg_conn.closed:
            self._pg_conn = psycopg2.connect(
                cursor_factory=self._cursor_factory, **self._dsn
            )
            self._pg_conn.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
            )
        return self._pg_conn

    @property
    def pg_cursor(self):
        """
        Opens a postgres cursor if it doesn't exist or is closed. Otherwise
        returns the current cursor.

        :return: a psycopg2 :class:`~psycopg2.extensions.cursor` instance or
            subclass as defined by the cursor_factory passed to the
            constructor
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
        Enquires as to whether this server is a master or a slave.

        :return: a boolean indicating whether the server is master.
        """
        with closing(self.pg_connection) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.execute('SELECT pg_is_in_recovery();')
                return not cursor.fetchone()[0]
