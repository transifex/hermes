from __future__ import absolute_import
from multiprocessing.queues import Queue
import unittest
from time import sleep
import select

from mock import MagicMock, patch
from psycopg2._psycopg import InterfaceError

from hermes.components import Component
from hermes.connectors import PostgresConnector
from hermes.strategies import AbstractErrorStrategy, CommonErrorStrategy
from tests import util
from tests.util import rand_string


_POSTGRES_DSN = {
    'database': 'test_hermes'
}


class ComponentTestCase(unittest.TestCase):
    def setUp(self):
        self.notif_queue = Queue(1)
        self.error_queue = Queue()
        self.component = Component(self.notif_queue._reader,
                                   CommonErrorStrategy(),
                                   self.error_queue,
                                   PostgresConnector(_POSTGRES_DSN))

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
        error_string = rand_string(10)

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
        error_string = rand_string(10)

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
