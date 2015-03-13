from __future__ import absolute_import
from Queue import Empty
from random import randint
from time import sleep
import os
from unittest import TestCase, skipUnless

from mock import MagicMock, patch

from signal import SIGINT, SIGCHLD

from hermes.client import Client
from hermes.components import Component
from hermes.connectors import PostgresConnector
from hermes.exceptions import InvalidConfigurationException


_WATCH_PATH = '/tmp/hermes_test'
_FAILOVER_FILES = ('recovery.conf', 'recovery.done')
_POSTGRES_DSN = {
    'database': 'test_hermes'
}


class ClientTestCase(TestCase):
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

    @skipUnless(os.environ.get('ALL_TESTS', False),
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

    @skipUnless(os.environ.get('ALL_TESTS', False),
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


class ClientComponentTestCase(TestCase):
    def test_add_listener_throws_on_non_component(self):
        client = Client(MagicMock(), MagicMock())
        self.assertRaises(InvalidConfigurationException,
                          client.add_listener,
                          3)

    def test_add_processor_throws_on_non_component(self):
        client = Client(MagicMock(), MagicMock())
        self.assertRaises(InvalidConfigurationException,
                          client.add_listener,
                          3)

    def test_add_listener_accepts_component(self):
        client = Client(MagicMock())
        client.add_listener(Component(MagicMock(),
                                      MagicMock(),
                                      MagicMock()))
        self.assertIsInstance(client._listener, Component)

    def test_add_processor_accepts_component(self):
        client = Client(MagicMock(), MagicMock())
        client.add_processor(Component(MagicMock(),
                                       MagicMock(),
                                       MagicMock()))
        self.assertIsInstance(client._processor, Component)


class ValidateComponentsTestCase(TestCase):
    def test_throws_on_non_listener(self):
        client = Client(MagicMock())
        client._processor = 3
        self.assertRaises(InvalidConfigurationException,
                          client._validate_components)

    def test_throws_on_non_processor(self):
        client = Client(MagicMock())
        client._listener = 3
        self.assertRaises(InvalidConfigurationException,
                          client._validate_components)

    def test_throws_on_different_queue(self):
        client = Client(MagicMock())

        client._listener = MagicMock()
        client._processor = MagicMock()

        client._listener.error_queue = MagicMock(
            return_value=True
        )
        client._listener.error_queue = MagicMock(
            return_value=False
        )
        self.assertRaises(InvalidConfigurationException,
                          client._validate_components)


class ClientStartupTestCase(TestCase):
    def test_startup_functions_are_called(self):
        with patch('multiprocessing.Process.start') as mock_process_start:
            with patch('hermes.client.signal') as mock_signal:
                client = Client(MagicMock())

                client._validate_components = MagicMock()

                client.start()

                mock_signal.assert_called_once_with(
                    SIGINT, client._handle_terminate
                )
                client._validate_components.assert_called_once_with()
                mock_process_start.assert_called_once_with()


class ClientRunProcedureTestCase(TestCase):
    def test_initial_run_funcs(self):
        with patch('hermes.log.get_logger'):
            with patch('hermes.client.signal') as mock_signal:
                with patch('select.select') as mock_select:
                    mock_select.side_effect = Exception

                    client = Client(MagicMock())
                    client._start_observer = MagicMock()
                    client.execute_role_based_procedure = MagicMock()

                    self.assertRaises(Exception, client.run)

                    mock_signal.assert_called_once_with(
                        SIGCHLD, client._handle_sigchld
                    )

                    client.execute_role_based_procedure.assert_called_once_with()

    def test_role_based_procedures_are_called_outside_of_main_loop(self):
        with patch('hermes.log.get_logger'):
            with patch('hermes.client.signal'):
                client = Client(MagicMock())

                random_raised = randint(1, 10000)
                client._exception_raised = random_raised

                client._start_observer = MagicMock()
                client.execute_role_based_procedure = MagicMock(
                    side_effect=Exception
                )

                self.assertRaises(Exception, client.run)

                client.execute_role_based_procedure.assert_called_once_with()

                # Raised value should be the same as that which we set
                self.assertEqual(client._exception_raised, random_raised)

    def test_client_calls_terminate_on_exit_queue(self):
        with patch('hermes.log.get_logger'):
            with patch('hermes.client.signal'):
                client = Client(MagicMock())
                client.execute_role_based_procedure = MagicMock()
                client._start_observer = MagicMock()
                client.terminate = MagicMock(side_effect=Exception)

                self.assertRaises(Empty, client._exit_queue.get_nowait)
                client._exit_queue.put(True)

                self.assertRaises(Exception, client.run)
                client.terminate.assert_called_once_with()
