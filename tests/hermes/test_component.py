from __future__ import absolute_import
from Queue import Empty
from multiprocessing.queues import Queue
import unittest
from time import sleep
import select
from errno import EINTR

from mock import MagicMock, patch
from psycopg2._psycopg import InterfaceError

from hermes.components import Component
from hermes.connectors import PostgresConnector

from hermes.strategies import ErrorStrategy
from tests import util
from tests.util import rand_string


_POSTGRES_DSN = {
    'database': 'test_hermes'
}


class TestSelectError(select.error):
    """
    A stub class to mock the behaviour of a select.error.

    Will always return the error number provided on cls(4)[n]
    """

    def __init__(self, error_no):
        self.error_no = error_no

    def __getitem__(self, item):
        return self.error_no


class ComponentTestCase(unittest.TestCase):
    def setUp(self):
        self.notif_queue = Queue(1)
        self.exit_queue = Queue(1)
        self.error_queue = Queue()
        self.component = Component(self.notif_queue._reader,
                                   ErrorStrategy(),
                                   self.exit_queue,
                                   self.error_queue,
                                   PostgresConnector(_POSTGRES_DSN))

    def tearDown(self):
        # Component can have an attribute error on _parent_pid due to the fact
        # that we defer super.__init__() until start()
        try:
            if self.component.is_alive():
                self.component.terminate()
        except AttributeError:
            pass

    @patch('hermes.components.select.select', side_effect=InterfaceError)
    def test_component_interface_error(self, select_module):
        # Start the component and let it settle
        self.component.start()
        sleep(1)
        self.assertFalse(self.component.is_alive())

    @patch('hermes.components.select.select',
           side_effect=TestSelectError(EINTR))
    def test_component_select_error(self, select_module):
        # Start the component and let it settle
        self.component.start()
        sleep(1)
        self.assertFalse(self.component.is_alive())

    @patch('hermes.components.select.select',
           side_effect=TestSelectError(10))
    def test_component_select_error(self, select_module):
        """
        Due to process memory isolation we must mock the cleaup to put a
        pre-defined string into the error queue.
        """
        error_string = rand_string(10)

        def mock_func(*args, **kwargs):
            """
            The process will have isolated this function, as well as the
            error queue.
            """
            self.component.error_queue.put(error_string)

        self.component.cleanup = MagicMock()
        self.component.cleanup.side_effect = mock_func

        self.component.start()
        sleep(1)
        return_string = self.component.error_queue.get()

        # Ensure the string PUT by the process is the same as what
        # was returned by queue.
        self.assertEqual(error_string, return_string)

    def test_not_implemented_exception_on_execute(self):
        exception_occurred = False
        try:
            self.component.execute()
        except NotImplementedError:
            exception_occurred = True
        self.assertTrue(exception_occurred)

    def test_not_implemented_exception_on_execute_done(self):
        exception_occurred = False
        try:
            self.component.execution_done()
        except NotImplementedError:
            exception_occurred = True
        self.assertTrue(exception_occurred)

    def test_exits_on_event_in_queue(self):
        self.component.start()
        self.component.exit_queue.put(1)
        sleep(1)
        self.assertFalse(self.component.is_alive())

    def test_execute_called_on_notification(self):
        error_string = rand_string(10)

        def mock_func(*args, **kwargs):
            """
            The process will have isolated this function, as well as the
            error queue.
            """
            self.component.error_queue.put(error_string)

        self.component.execute = MagicMock()
        self.component.execute.side_effect = mock_func

        self.component.execution_done = MagicMock()

        self.component.start()
        self.notif_queue.put(1)

        return_string = self.error_queue.get()
        self.assertEqual(error_string, return_string)

    def test_execute_done_called_on_notification(self):
        error_string = rand_string(10)

        def mock_func(*args, **kwargs):
            """
            The process will have isolated this function, as well as the
            error queue.
            """
            self.component.error_queue.put(error_string)

        self.component.execution_done = MagicMock()
        self.component.execution_done.side_effect = mock_func

        self.component.execute = MagicMock()

        self.component.start()
        self.notif_queue.put(1)

        return_string = self.error_queue.get()
        self.assertEqual(error_string, return_string)

    def test_error_received_on_exception_in_execute(self):
        exception_msg = util.rand_string(10)

        error_strat = ErrorStrategy()
        error_strat.handle_exception = MagicMock(
            return_value=(False, exception_msg)
        )
        self.component.error_strategy = error_strat
        self.component.execute = MagicMock(
            side_effect=Exception(exception_msg)
        )

        # Start the component and let it settle
        self.component.start()
        self.notif_queue.put(True)

        handled, message = self.error_queue.get()

        self.assertFalse(handled)
        self.assertEqual(exception_msg, message)

    def test_component_process_reuse(self):
        self.component.start()
        sleep(1)
        self.component.terminate()
        sleep(1)
        self.component.start()
        sleep(1)
        self.assertTrue(self.component.is_alive())

    def test_empty_queue_function(self):
        self.exit_queue.put(1)
        self.component.__empty_exit_queue__()
        exception_raised = False
        try:
            self.exit_queue.get_nowait()
        except Empty:
            exception_raised = True
        self.assertTrue(exception_raised)
