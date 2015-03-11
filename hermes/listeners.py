from Queue import Full

from components import Component


class PostgresNotificationListener(Component):
    """
    A listener to detect event notifications from Postgres and pass onto to
    a processor.
    """

    def __init__(self, pg_connector, notif_channel, notif_queue,
                 error_strategy, error_queue, fire_on_start=True):
        """
        :param pg_connector: A :class:`~hermes.connectors.PostgresConnector`
            object
        :param notif_channel: The string representing the notification channel
            to listen to updates on
        :param notif_queue: A :class:`~multiprocessing.Queue` to be used for
            notification events.
        :param error_strategy: A
            :class:`~hermes.strategies.CommonErrorStrategy` subclass
        :param error_queue: A :class:`~multiprocessing.Queue` to be used for
            error events.
        """
        super(PostgresNotificationListener, self).__init__(
            pg_connector.pg_connection, error_strategy, error_queue
        )
        self._fire_on_start = fire_on_start
        self.notif_channel = notif_channel
        self.notif_queue = notif_queue
        self.pg_connector = pg_connector

    def set_up(self):
        super(PostgresNotificationListener, self).set_up()
        self.notification_pipe = self.pg_connector.pg_connection

        self.pg_connector.pg_cursor.execute(
            "LISTEN {};".format(self.notif_channel)
        )

        if self._fire_on_start:
            try:
                self.notif_queue.put_nowait(True)
            except Full:
                pass

    def execute(self, pre_exec_value):
        self.pg_connector.pg_connection.poll()
        while self.pg_connector.pg_connection.notifies:
            self.pg_connector.pg_connection.notifies.pop()
            try:
                self.notif_queue.put_nowait(True)
            except Full:
                pass

    def tear_down(self):
        super(PostgresNotificationListener, self).tear_down()
        self.pg_connector.disconnect()
