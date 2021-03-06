from multiprocessing.queues import Queue
from random import randint
from unittest import TestCase
from time import sleep
import select
import os
import multiprocessing
from signal import SIGTERM, SIGINT

from mock import MagicMock, patch
from psycopg2._psycopg import InterfaceError

from hermes.components import Component
from hermes.connectors import PostgresConnector
from hermes.strategies import AbstractErrorStrategy, CommonErrorStrategy, \
    TERMINATE, BACKOFF, CONTINUE
from test_hermes.util import LimitedTrueBool
import util


_POSTGRES_DSN = {
    'database': 'test_hermes'
}


class ComponentTestCase(TestCase):
    def setUp(self):
        self.notif_queue = Queue(1)
        self.error_queue = Queue()
        self.component = Component(self.notif_queue._reader,
                                   CommonErrorStrategy(),
                                   self.error_queue,
                                   PostgresConnector(_POSTGRES_DSN))
        self.component.log = MagicMock()

    def tearDown(self):
        # Component can have an attribute error on _parent_pid due to the fact
        # that we defer super.__init__() until start()
        if self.component.is_alive():
            self.component.terminate()
            self.component.join()

    @patch('hermes.components.select.select', side_effect=InterfaceError)
    def test_component_interface_error(self, select_module):
        # Start the component and let it settle
        self.component.start()
        sleep(1)
        self.assertFalse(self.component.is_alive())

    def test_component_select_error(self):
        """
        Due to process memory isolation we must mock the cleaup to put a
        pre-defined string into the error queue.
        """
        with patch('hermes.components.select.select',
                   side_effect=select.error):
            self.component.start()
            sleep(1)

            # Ensure the string PUT by the process is the same as what
            # was returned by queue.
            self.assertFalse(self.component.is_alive())

    def test_not_implemented_exception_on_execute(self):
        exception_occurred = False
        try:
            self.component.execute(None)
        except NotImplementedError:
            exception_occurred = True
        self.assertTrue(exception_occurred)

    def test_exits_on_terminate(self):
        self.component.start()
        sleep(1)
        self.component.terminate()
        sleep(1)
        self.assertFalse(self.component.is_alive())

    def test_execute_called_on_notification(self):
        error_string = util.rand_string(10)

        def mock_func(*args, **kwargs):
            """
            The process will have isolated this function, as well as the
            error queue.
            """
            self.component.error_queue.put(error_string)

        with patch('hermes.components.Component.execute') as mock_execute:
            mock_execute.side_effect = mock_func

            self.component.start()
            sleep(2)
            self.assertTrue(self.component.is_alive())

            self.notif_queue.put(1)

            return_string = self.error_queue.get(timeout=2)
            self.assertEqual(error_string, return_string)

    def test_execute_done_called_on_notification(self):
        error_string = util.rand_string(10)

        def mock_func(*args, **kwargs):
            """
            The process will have isolated this function, as well as the
            error queue.
            """
            self.component.error_queue.put(error_string)

        self.component.post_execute = MagicMock()
        self.component.post_execute.side_effect = mock_func

        self.component.execute = MagicMock()

        self.component.start()
        self.notif_queue.put(1)

        return_string = self.error_queue.get()
        self.assertEqual(error_string, return_string)

    def test_error_received_on_exception_in_execute(self):
        mock_execption_return = (False, util.rand_string(10))

        error_strat = AbstractErrorStrategy()
        error_strat.handle_exception = MagicMock(
            return_value=mock_execption_return
        )

        with patch('hermes.components.Component.execute',
                   side_effect=Exception):
            self.component.error_strategy = error_strat

            # Start the component and let it settle
            self.component.start()
            sleep(1)
            self.notif_queue.put(True)

            exception = self.error_queue.get(timeout=1)

        self.assertEqual(mock_execption_return, exception)

    def test_component_process_reuse(self):
        self.component.start()
        sleep(1)
        self.component.terminate()
        self.component.join()
        sleep(1)
        self.component.start()
        sleep(1)
        self.assertTrue(self.component.is_alive())

    def test_isalive_is_false_on_attr_error(self):
        self.assertRaises(AttributeError, lambda: self.component._popen)
        return_value = self.component.is_alive()
        self.assertFalse(return_value)

    def test_ident_is_none_on_attr_error(self):
        self.assertRaises(AttributeError, lambda: self.component._popen)
        return_value = self.component.ident
        self.assertIsNone(return_value)

    def test_join_returns_on_no_process(self):
        self.assertRaises(AttributeError, lambda: self.component._popen)
        return_value = self.component.join()
        self.assertIsNone(return_value)

    def test_execute_gets_notification_and_calls_execute_funcs(self):
        self.component._should_run = LimitedTrueBool(1)
        self.component._backoff_time = randint(1, 10000)

        self.component.execute = MagicMock()
        self.component.post_execute = MagicMock()
        self.component.pre_execute = MagicMock()

        # Put the notification so it will immediately return from select
        self.notif_queue.put(True)

        self.component._execute()

        self.assertEqual(self.component.post_execute.call_count, 1)
        self.assertEqual(self.component.execute.call_count, 1)
        self.assertEqual(self.component.pre_execute.call_count, 1)

        self.assertEqual(self.component.__backoff_time__, 0)

    def test_main_loop_is_called(self):
        with patch('hermes.log.get_logger'):
            with patch('multiprocessing.Process.start'):
                self.component.set_up = MagicMock()
                self.component._execute = MagicMock()
                self.component.tear_down = MagicMock(side_effect=Exception)
                self.component.start()

                self.assertRaises(Exception, self.component.run)

                self.assertEqual(self.component.set_up.call_count, 1)
                self.assertEqual(self.component._execute.call_count, 1)
                self.assertEqual(self.component.tear_down.call_count, 1)

    def test_breaks_on_interrupt(self):
        with patch('hermes.log.get_logger'):
            with patch('multiprocessing.Process.start'):
                self.component.set_up = MagicMock(side_effect=select.error)
                self.component.tear_down = MagicMock(side_effect=Exception)
                self.component.start()

                self.assertRaises(Exception, self.component.run)

                self.assertEqual(self.component.set_up.call_count, 1)
                self.assertEqual(self.component.tear_down.call_count, 1)


class SignalHandlerTestCase(TestCase):
    def test_setup_signal_handlers(self):
        component = Component(MagicMock(), MagicMock(), MagicMock())
        component._handle_stop_signal = MagicMock()
        component.set_up()

        current_pid = multiprocessing.current_process().pid

        os.kill(current_pid, SIGTERM)
        self.assertEqual(component._handle_stop_signal.call_count, 1)
        component._handle_stop_signal.reset_mock()

        os.kill(current_pid, SIGINT)
        self.assertEqual(component._handle_stop_signal.call_count, 1)

    def test_should_run_is_set_to_false_on_stop_signal(self):
        component = Component(MagicMock(), MagicMock(), MagicMock())
        component._should_run = True
        component._handle_stop_signal(None, None)
        self.assertFalse(component._should_run)


class RunExceptionHandlingTestCase(TestCase):
    def test_continue_re_runs_main_loop(self):
        with patch('hermes.log.get_logger'):
            with patch('multiprocessing.Process.start'):
                component = Component(MagicMock(), MagicMock(), MagicMock())
                component.log = MagicMock()

                component._execute = MagicMock(side_effect=Exception)
                component.error_strategy.handle_exception = MagicMock(
                    return_value=(None, CONTINUE)
                )
                component.tear_down = MagicMock(side_effect=Exception)
                component.set_up = MagicMock()
                component.start()
                self.assertRaises(Exception, component.run)

                self.assertEqual(component.tear_down.call_count, 1)
                self.assertEqual(component._execute.call_count, 1)

                self.assertTrue(component._should_run)

    def test_backoff_calls_backoff_function_and_continues(self):
        with patch('hermes.log.get_logger'):
            with patch('multiprocessing.Process.start'):
                component = Component(MagicMock(), MagicMock(), MagicMock())
                component.log = MagicMock()

                component._execute = MagicMock(side_effect=Exception)
                component.error_strategy.handle_exception = MagicMock(
                    return_value=(None, BACKOFF)
                )
                component._backoff = MagicMock()
                component.tear_down = MagicMock(side_effect=Exception)
                component.set_up = MagicMock()
                component.start()

                self.assertRaises(Exception, component.run)
                self.assertEqual(component.tear_down.call_count, 1)
                self.assertEqual(component._execute.call_count, 1)
                self.assertTrue(component._should_run)
                self.assertEqual(component._backoff.call_count, 1)

    def test_expected_terminate_cancels_main_loop(self):
        with patch('hermes.log.get_logger'):
            with patch('multiprocessing.Process.start'):
                component = Component(MagicMock(), MagicMock(), MagicMock())
                component.log = MagicMock()

                component._execute = MagicMock(side_effect=Exception)
                component.error_strategy.handle_exception = MagicMock(
                    return_value=(True, TERMINATE)
                )
                component._backoff = MagicMock()
                component.tear_down = MagicMock(side_effect=Exception)
                component.set_up = MagicMock()
                component.start()

                self.assertRaises(Exception, component.run)
                self.assertEqual(component.tear_down.call_count, 1)
                self.assertEqual(component._execute.call_count, 1)
                self.assertFalse(component._should_run)

    def test_unexpected_terminate_cancels_main_loop(self):
        with patch('hermes.log.get_logger'):
            with patch('multiprocessing.Process.start'):
                component = Component(MagicMock(), MagicMock(), MagicMock())
                component.log = MagicMock()

                component._execute = MagicMock(side_effect=Exception)
                component.error_strategy.handle_exception = MagicMock(
                    return_value=(False, TERMINATE)
                )
                component._backoff = MagicMock()
                component.tear_down = MagicMock(side_effect=Exception)
                component.set_up = MagicMock()
                component.start()

                self.assertRaises(Exception, component.run)
                self.assertEqual(component.tear_down.call_count, 1)
                self.assertEqual(component._execute.call_count, 1)
                self.assertFalse(component._should_run)


class BackoffTimingTestCase(TestCase):
    def test_backoff_is_one_on_zero(self):
        with patch('hermes.components.sleep') as mock_sleep:
            component = Component(MagicMock(), MagicMock(), MagicMock())
            component.log = MagicMock()
            component._backoff()

            mock_sleep.assert_called_once_with(component.__backoff_time__)
            self.assertEqual(component.__backoff_time__, 1)

    def test_backoff_resets_to_one_when_over_limt(self):
        with patch('hermes.components.sleep') as mock_sleep:
            component = Component(MagicMock(), MagicMock(), MagicMock())
            component.log = MagicMock()

            component.__backoff_time__ = component._backoff_limit + 1
            component._backoff()

            mock_sleep.assert_called_once_with(component.__backoff_time__)
            self.assertEqual(component.__backoff_time__, 1)

    def test_binary_backoff_implemented(self):
        with patch('hermes.components.sleep') as mock_sleep:
            component = Component(MagicMock(), MagicMock(), MagicMock())
            component.log = MagicMock()

            component.__backoff_time__ = randint(1, 1000)
            component._backoff_limit = component.__backoff_time__ << 3
            expected_value = component.__backoff_time__ << 1

            component._backoff()

            mock_sleep.assert_called_once_with(expected_value)
            self.assertEqual(component.__backoff_time__, expected_value)
