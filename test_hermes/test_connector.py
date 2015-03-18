from __future__ import absolute_import
import unittest

from mock import MagicMock, patch, PropertyMock

from hermes.connectors import PostgresConnector


_POSTGRES_DSN = {
    'database': 'test_hermes'
}


class ConnectorTestCase(unittest.TestCase):
    def setUp(self):
        self.pg_connector = PostgresConnector(_POSTGRES_DSN)

    def tearDown(self):
        self.pg_connector.disconnect()
        self.pg_connector = None

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

    def test_is_server_master_returns_true_on_pgfunc_false(self):
        with patch('hermes.connectors.PostgresConnector.pg_connection',
                   new_callable=PropertyMock) as prop_conn:
            self.setUp()
            mock_cursor = MagicMock()

            mock_cursor.fetchone.return_value = [False]
            prop_conn.return_value.cursor.return_value = mock_cursor

            return_value = self.pg_connector.is_server_master()
            self.assertTrue(return_value)

    def test_is_server_master_returns_false_on_pgfunc_true(self):
        with patch('hermes.connectors.PostgresConnector.pg_connection',
                   new_callable=PropertyMock) as prop_conn:
            self.setUp()
            mock_cursor = MagicMock()

            mock_cursor.fetchone.return_value = [True]
            prop_conn.return_value.cursor.return_value = mock_cursor

            return_value = self.pg_connector.is_server_master()
            self.assertFalse(return_value)
