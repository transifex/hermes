from __future__ import absolute_import
from Queue import Empty
from random import randint
from time import sleep
import os
from unittest import TestCase, skipUnless
from signal import SIGINT, SIGCHLD
from select import error as select_error

from mock import MagicMock, patch, PropertyMock
from os import getpid

from hermes.client import Client
from hermes.components import Component
from hermes.connectors import PostgresConnector
from hermes.exceptions import InvalidConfigurationException


_WATCH_PATH = '/tmp/hermes_test'
_FAILOVER_FILES = ('recovery.conf', 'recovery.done')
_POSTGRES_DSN = {
    'database': 'test_hermes'
}


class RunningClientTestCase(TestCase):
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
                          client.add_processor,
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


class WatchdogObserverTestCase(TestCase):
    def setUp(self):
        self.client = Client(MagicMock())
        self.client.directory_observer = MagicMock()

    def test_start_schedules_obeserver_if_watch_path(self):
        self.client._watch_path = randint(50, 1000)

        self.client._start_observer()
        self.client.directory_observer.schedule.assert_called_once_with(
            self.client, self.client._watch_path, recursive=False
        )
        self.client.directory_observer.start.assert_called_once_with()

    def test_start_not_schedule_observer_if_none_watch_path(self):
        self.client._watch_path = None

        self.client._start_observer()
        self.assertEqual(self.client.directory_observer.schedule.call_count, 0)
        self.assertEqual(self.client.directory_observer.start.call_count, 0)

    def test_stop_stops_observer_if_watch_path_and_observer(self):
        self.client.directory_observer.is_alive.return_value = True
        self.client._watch_path = True
        self.client._stop_observer()
        self.client.directory_observer.stop.assert_called_once_with()

    def test_stop_does_not_stop_observer_on_none(self):
        self.client._watch_path = None
        self.client._stop_observer()
        self.assertEqual(self.client.directory_observer.stop.call_count, 0)

    def test_stop_does_not_stop_on_dead(self):
        self.client._watch_path = True
        self.client.directory_observer.is_alive.return_value = False
        self.client._stop_observer()
        self.assertEqual(self.client.directory_observer.stop.call_count, 0)


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

    def test_initial_start_components(self):
        client = Client(MagicMock())

        client._processor = MagicMock()
        client._processor.is_alive.return_value = False

        client._listener = MagicMock()
        client._listener.is_alive.return_value = False

        client._start_components()
        client._listener.start.assert_called_once_with()
        client._processor.start.assert_called_once_with()

    def test_start_components_when_components_running(self):
        client = Client(MagicMock())

        client._processor = MagicMock()
        client._processor.is_alive.return_value = True

        client._listener = MagicMock()
        client._listener.is_alive.return_value = True

        client._start_components()
        self.assertEqual(client._listener.start.call_count, 0)
        self.assertEqual(client._processor.start.call_count, 0)

    def test_join_is_called_on_restart(self):
        client = Client(MagicMock())

        client._processor = MagicMock()
        client._processor.is_alive.return_value = False
        client._processor.ident.return_value = True

        client._listener = MagicMock()
        client._listener.is_alive.return_value = False
        client._listener.ident.return_value = True

        client._start_components(restart=True)
        client._listener.join.assert_called_once_with()
        client._processor.join.assert_called_once_with()


class ClientShutdownTestCase(TestCase):
    def test_shutdown(self):
        client = Client(MagicMock())
        client.log = MagicMock()
        client._stop_components = MagicMock()
        client._stop_observer = MagicMock()
        client._should_run = True

        client._shutdown()
        client._stop_components.assert_called_once_with()
        client._stop_observer.assert_called_once_with()
        self.assertFalse(client._should_run)

    def test_stop_terminates(self):
        client = Client(MagicMock())

        client._processor = MagicMock()
        client._listener = MagicMock()

        client._processor.ident.return_value = True
        client._listener.ident.return_value = True

        client._processor.is_alive.return_value = True
        client._listener.is_alive.return_value = True

        client._stop_components()

        client._processor.terminate.assert_called_once_with()
        client._listener.terminate.assert_called_once_with()
        client._listener.join.assert_called_once_with()
        client._processor.join.assert_called_once_with()

    def test_handle_terminate_when_same_process(self):
        with patch('hermes.client.Client.ident',
                   new_callable=PropertyMock) as mock_ident:
            client = Client(MagicMock())
            client._shutdown = MagicMock()
            mock_ident.return_value = getpid()

            client._handle_terminate(None, None)
            client._shutdown.assert_called_once_with()

    def test_handle_terminate_when_different_process(self):
        with patch('hermes.client.Client.ident',
                   new_callable=PropertyMock) as mock_ident:
            client = Client(MagicMock())
            client._exit_queue = MagicMock()
            client._shutdown = MagicMock()

            current_pid = getpid()
            mock_ident.return_value = current_pid + 1

            client._handle_terminate(None, None)
            client._exit_queue.put_nowait.assert_called_once_with(True)


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

    def test_client_sets_run_flag_on_interrupt(self):
        with patch('hermes.log.get_logger'):
            with patch('select.select', side_effect=select_error):
                client = Client(MagicMock())
                client.execute_role_based_procedure = MagicMock()
                client.run()
                self.assertFalse(client._should_run)
