from __future__ import absolute_import
from time import sleep
import unittest
import os

from mock import MagicMock

from hermes.client import Client
from hermes.connectors import PostgresConnector


_WATCH_PATH = '/tmp/hermes_test'
_FAILOVER_FILES = ('recovery.conf', 'recovery.done')
_POSTGRES_DSN = {
    'database': 'test_hermes'
}


class ClientTestCase(unittest.TestCase):
    def setUp(self):
        # Create the folder
        if not os.path.exists(_WATCH_PATH):
            os.makedirs(_WATCH_PATH)
        self.client = Client(_POSTGRES_DSN, _WATCH_PATH, _FAILOVER_FILES)
        self.client.log = MagicMock()

    def tearDown(self):
        if self.client.is_alive():
            self.client.terminate()
        # Remove the folder
        if not os.path.exists(_WATCH_PATH):
            os.removedirs(_WATCH_PATH)

    @unittest.skipUnless(os.environ.get('TX_ALL_TESTS', False),
                         "Unittests only")
    def test_client_directory_watcher_when_server_master(self):
        # We have to monkey patch the 'is_server_master' function to ensure
        # we can control the test path
        old_func = PostgresConnector.is_server_master
        PostgresConnector.is_server_master = MagicMock(return_value=True)

        self.client._start_components = MagicMock(return_value=None)

        # Start the client and allow to settle
        self.client._start_observer()
        sleep(3)

        # Create a file and detect if the RiverClient has been informed
        file_path = '{}/{}'.format(_WATCH_PATH, _FAILOVER_FILES[0])
        with open(file_path, 'a'):
            os.utime(file_path, None)

        # Give the event time to emit
        sleep(3)

        self.assertTrue(self.client._start_components.called)

        PostgresConnector.is_server_master = old_func

    @unittest.skipUnless(os.environ.get('TX_ALL_TESTS', False),
                         "Unittests only")
    def test_client_directory_watcher_when_server_slave(self):
        # We have to monkey patch the 'is_server_master' function to ensure
        # we can control the test path
        old_func = PostgresConnector.is_server_master
        PostgresConnector.is_server_master = MagicMock(return_value=False)
        # Start the observer and allow to settle
        self.client.directory_observer.start()
        sleep(3)

        # Create a file and detect if the RiverClient has been informed
        file_path = '{}/{}'.format(_WATCH_PATH, _FAILOVER_FILES[0])
        with open(file_path, 'a'):
            os.utime(file_path, None)

        # Give the event time to emit
        sleep(3)

        self.assertFalse(self.client.is_alive())

        PostgresConnector.is_server_master = old_func

    def test_client_directory_watcher_when_file_incorrect(self):
        # We have to monkey patch the 'is_server_master' function to ensure
        # we can control the test path
        old_func = PostgresConnector.is_server_master
        PostgresConnector.is_server_master = MagicMock(return_value=True)

        # Start the observer and allow to settle
        self.client.directory_observer.start()
        sleep(3)

        # Create a file and detect if the RiverClient has been informed
        file_path = '{}/{}'.format(_WATCH_PATH, 'random_file.rand')
        with open(file_path, 'a'):
            os.utime(file_path, None)

        # Give the event time to emit
        sleep(3)

        self.assertFalse(PostgresConnector.is_server_master.called)

        PostgresConnector.is_server_master = old_func
