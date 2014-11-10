from Queue import Full
from hermes.log import logger
from components import Component


class NotificationListener(Component):
    """
    A listener to detect event notifications from Postgres and pass onto to
    a processor.
    """

    def __init__(self, pg_connector, notif_channel, notif_queue,
                 error_strategy, error_queue, exit_queue, fire_on_start=True):
        """

        :param pg_connector: a PostgresConnector object.

        :param notif_channel: the string representing the notification channel
        to listen to updates on.

        :param notif_queue: the notification queue to pass events to

        :param error_strategy: the error strategy that this listener should
        follow.

        :param error_queue: the error queue to inform the client on.

        :param exit_queue: the queue to listen for exit events on.
        """
        super(NotificationListener, self).__init__(
            pg_connector.pg_connection, error_strategy,
            exit_queue, error_queue, pg_connector
        )
        self._fire_on_start = fire_on_start
        self.notif_channel = notif_channel
        self.notif_queue = notif_queue

    def run(self):
        logger.info("Waiting for notifications on channel '{}'".format(
            self.notif_channel
        ))
        self.pg_connector.pg_cursor.execute(
            "LISTEN {};".format(self.notif_channel)
        )

        if self._fire_on_start:
            # Force a notification to ensure the processor runs at start.
            try:
                self.notif_queue.put_nowait(True)
            except Full:
                pass

        super(NotificationListener, self).run()

    def execute(self):
        self.pg_connector.pg_connection.poll()
        while self.pg_connector.pg_connection.notifies:
            self.pg_connector.pg_connection.notifies.pop()
            try:
                self.notif_queue.put_nowait(True)
            except Full:
                pass

    def execution_done(self):
        pass
