from __future__ import absolute_import
from Queue import Empty
from contextlib import closing
from multiprocessing.queues import Queue
from time import sleep
import unittest

from hermes.connectors import PostgresConnector
from hermes.listeners import HermesNotificationListener
from hermes.strategies import ErrorStrategy


_POSTGRES_DSN = {
    'database': 'test_hermes'
}
_NOTIF_CHANNEL = 'test_herms_notif'


class ListenerTestCase(unittest.TestCase):
    def setUp(self):
        pg_connector = PostgresConnector(_POSTGRES_DSN)
        self.notif_queue = Queue(1)
        self.listener = HermesNotificationListener(
            pg_connector, _NOTIF_CHANNEL, self.notif_queue,
            ErrorStrategy(), Queue(), Queue(1), fire_on_start=False
        )

    def tearDown(self):
        self.listener.terminate()

    def test_notification_listener(self):
        self.assertTrue(self.notif_queue.empty())
        # Start the listener process
        self.listener.start()
        sleep(3)

        send_notification()

        notif = self.notif_queue.get(timeout=5)
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
        sleep(3)
        self.listener.terminate()
        self.assertFalse(self.listener.is_alive())


def send_notification():
    with closing(PostgresConnector(_POSTGRES_DSN).pg_connection) as conn:
        with closing(conn.cursor()) as cursor:
            sql = "NOTIFY {}, '1234';".format(
                _NOTIF_CHANNEL
            )
            cursor.execute(sql)
