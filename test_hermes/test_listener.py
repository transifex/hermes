from __future__ import absolute_import
from Queue import Empty, Full
from contextlib import closing
from multiprocessing.queues import Queue
from random import randint
from time import sleep
from unittest import TestCase

from mock import MagicMock, patch

from hermes.connectors import PostgresConnector
from hermes.listeners import PostgresNotificationListener
from hermes.strategies import CommonErrorStrategy
from test_hermes.util import LimitedTrueBool


_POSTGRES_DSN = {
    'database': 'test_hermes'
}
_NOTIF_CHANNEL = 'test_herms_notif'


class RunningListenerTestCase(TestCase):
    def setUp(self):
        pg_connector = PostgresConnector(_POSTGRES_DSN)
        self.notif_queue = Queue(1)
        self.listener = PostgresNotificationListener(
            pg_connector, _NOTIF_CHANNEL, self.notif_queue,
            CommonErrorStrategy(), Queue(), fire_on_start=False
        )
        self.listener.log = MagicMock()

    def tearDown(self):
        self.listener.terminate()
        self.listener.join()

    def test_notification_listener(self):
        self.assertTrue(self.notif_queue.empty())
        # Start the listener process
        self.listener.start()
        sleep(1)

        send_notification()

        try:
            error = self.listener.error_queue.get(timeout=1)
            self.assertTrue(False, error)
        except Empty:
            pass

        notif = self.notif_queue.get(timeout=2)
        self.assertIsNotNone(notif)

    def test_notification_stops_listening_on_terminate(self):
        self.assertTrue(self.notif_queue.empty())

        self.listener.start()
        sleep(3)
        self.listener.terminate()

        # Send a notification and ensure the listener is NOT listening
        send_notification()
        exception_raised = False
        try:
            self.notif_queue.get(timeout=5)
        except Empty:
            exception_raised = True
        self.assertTrue(exception_raised)

    def test_notification_listener_terminates(self):
        self.assertTrue(self.notif_queue.empty())
        self.listener.start()
        sleep(1)
        self.listener.terminate()
        sleep(1)
        self.assertFalse(self.listener.is_alive())


def send_notification():
    with closing(PostgresConnector(_POSTGRES_DSN).pg_connection) as conn:
        with closing(conn.cursor()) as cursor:
            sql = "NOTIFY {}, '1234';".format(
                _NOTIF_CHANNEL
            )
            cursor.execute(sql)


class FunctionalListenerTestCase(TestCase):
    def setUp(self):
        self.listener = PostgresNotificationListener(
            MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
        )

    def tearDown(self):
        self.listener = None

    def test_setup_executes_listen_command(self):
        self.listener.set_up()

        self.assertEqual(
            self.listener.notification_pipe,
            self.listener.pg_connector.pg_connection
        )
        self.assertEqual(
            self.listener.pg_connector.pg_cursor.execute.call_count, 1
        )

    def test_setup_fires_if_set(self):
        self.listener._fire_on_start = True
        self.listener.set_up()
        self.listener.notif_queue.put_nowait.assert_called_once_with(True)

        # Check Full exception is ignored
        self.listener.notif_queue.put_nowait.side_effect = Full
        self.listener.notif_queue.put_nowait.assert_called_once_with(True)

    def test_execute_pops_number_of_notifications(self):
        no_of_notifications = randint(20, 2000)

        limited_bool = LimitedTrueBool(no_of_notifications)
        limited_bool.pop = MagicMock()

        self.listener.pg_connector.pg_connection.notifies = limited_bool
        self.listener.execute(None)

        self.listener.pg_connector.pg_connection.poll.assert_called_once_with()
        self.assertEqual(
            self.listener.pg_connector.pg_connection.notifies.pop.call_count,
            no_of_notifications
        )
        self.assertEqual(self.listener.notif_queue.put_nowait.call_count,
                         no_of_notifications)

    def test_execute_ignores_full_exception(self):
        limited_bool = LimitedTrueBool(1)
        limited_bool.pop = MagicMock()
        self.listener.pg_connector.pg_connection.notifies = limited_bool
        self.listener.notif_queue.put_nowait.side_effect = Full

        self.listener.execute(None)

        self.listener.notif_queue.put_nowait.assert_called_once_with(True)

    def test_tear_down_calls_super(self):
        with patch('hermes.components.Component.tear_down') as mock_tear:
            self.listener.tear_down()
            mock_tear.assert_called_once_with()

    def tear_tear_down_disconnects_pg_connector(self):
        self.listener.tear_down()
        self.listener.pg_connector.disconnect.assert_called_once_with()
