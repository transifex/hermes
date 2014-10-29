from __future__ import absolute_import
import unittest

from hermes.connectors import PostgresConnector

_POSTGRES_DSN = {
    'database': 'test_hermes'
}


class ConnectorTestCase(unittest.TestCase):
    def setUp(self):
        self.pg_connector = PostgresConnector(_POSTGRES_DSN)

    def tearDown(self):
        self.pg_connector.disconnect()

    def test_connector_not_connected_on_init(self):
        self.assertIsNone(self.pg_connector._pg_conn)
        self.assertIsNone(self.pg_connector._pg_cursor)

    def test_connector_connects(self):
        connection = self.pg_connector.pg_connection
        cursor = self.pg_connector.pg_cursor

        self.assertIsNotNone(connection)
        self.assertIsNotNone(cursor)

        self.assertFalse(connection.closed)
        self.assertFalse(cursor.closed)
