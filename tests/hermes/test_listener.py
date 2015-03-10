from __future__ import absolute_import
from Queue import Empty
from contextlib import closing
from multiprocessing.queues import Queue
from time import sleep
import unittest

from mock import MagicMock

from hermes.connectors import PostgresConnector
from hermes.listeners import NotificationListener
from hermes.strategies import CommonErrorStrategy


_POSTGRES_DSN = {
    'database': 'test_hermes'
}
_NOTIF_CHANNEL = 'test_herms_notif'


class ListenerTestCase(unittest.TestCase):
    def setUp(self):
        pg_connector = PostgresConnector(_POSTGRES_DSN)
        self.notif_queue = Queue(1)
        self.listener = NotificationListener(
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
