# -*- coding: utf-8 -*-
from Queue import Empty
from multiprocessing.process import Process
from select import select
from time import sleep

from psycopg2._psycopg import OperationalError
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from hermes.components import Component
from hermes.connectors import PostgresConnector
from hermes.log import logger
from hermes.exceptions import InvalidConfigurationException


class Client(Process, FileSystemEventHandler):
    """
    Hermes client. Responsible for Listener and Processor components. Provides
    functions to start/stop both itself and its components. In addition, it is
    also capable of receiving file-system events via the 'watchdog' library.

    General procedure::

        1. Starts both the Process and Listener components.
        2. Listen and act upon exit/error notifications from components
        3. Listen for file-system events and act accordingly.
    """
    def __init__(self, dsn, watch_path=None, failover_files=None):
        """
        To make the client listen for Postgres 'recovery.conf, recovery.done'
        events::
            from hermes.client import Client

            dsn = {'database': 'example_db',
                   'host': '127.0.0.1',
                   'port': 5432,
                   'user': 'example',
                   'password': 'example'}

            watch_path = '/var/lib/postgresql/9.4/main/'
            failover_files = ['recovery.done', 'recovery.conf']

            client = Client(dsn, watch_path, failover_files)

            # Add processor and listener
            ...

            # Start the client
            client.start()

        Or, if you decide you don't want to use a file watcher, then you
        can omit those parameters. However, the Client will still perform
        master/slave checks if a problem is encountered::
            from hermes.client import Client

            dsn = {'database': 'example_db',
                   'host': '127.0.0.1',
                   'port': 5432,
                   'user': 'example',
                   'password': 'example'}

            client = Client(dsn)

            # Add processor and listener
            ...

            # Start the client
            client.start()



        :param dsn: a Postgres-compatible DSN dictionary
        :param watch_path: the directory to monitor for filechanges. If None,
            then file monitoring is disabled.
        :param failover_files: a list of files which, when modified, will
            cause the client to call :func:`~execute_role_based_procedure`
        """
        super(Client, self).__init__()

        self.directory_observer = Observer()

        self._processor = None
        self._listener = None

        self._started = False
        self._should_run = True

        self._watch_path = watch_path
        self._failover_files = failover_files
        self.master_pg_conn = PostgresConnector(dsn)

    def add_processor(self, processor):
        """
        :param processor: A :class:`~hermes.components.Component` object which
            will receive notifications and run the
            :func:`~hermes.components.Component.execute` method.

        :raises: :class:`~hermes.exceptions.InvalidConfigurationException` if
            the provided processor is not a subclass of
            :class:`~hermes.components.Component`
        """
        if not isinstance(processor, Component):
            raise InvalidConfigurationException(
                "Processor must of type Component"
            )
        self._processor = processor

    def add_listener(self, listener):
        """
        :param listener: A :class:`~hermes.components.Component` object which
            will listen for notifications from Postgres and pass an event down
            a queue.

        :raises: :class:`~hermes.exceptions.InvalidConfigurationException` if
            the provided listener is not a subclass of
            :class:`~hermes.components.Component`
        """
        if not isinstance(listener, Component):
            raise InvalidConfigurationException(
                "Listener must of type Component"
            )
        self._listener = listener

    def _validate_components(self):
        """
        Checks through a set of validation procedures to ensure the client is
        configured properly.

        :raises: :class:`~hermes.exceptions.InvalidConfigurationException`
        """
        if not self._processor:
            raise InvalidConfigurationException("A processor must be defined")

        if not self._listener:
            raise InvalidConfigurationException("A listener must be defined")

        if self._processor.error_queue is not self._listener.error_queue:
            raise InvalidConfigurationException(
                "A processor and listener's error queue must be the same"
            )

    def start(self):
        """
        Starts the Client, its Components and the directory observer

        :raises: :class:`~hermes.exceptions.InvalidConfigurationException`
        """
        self._validate_components()
        if not self._started:
            self._start_observer()
        super(Client, self).start()

    def run(self):
        """
        Performs a :func:`~select.select` on the components' error queue.
        When a notification is detected, the client will log the message and
        then calculate if the Postgres server is still a Master -  if not, the
        components are shutdown.
        """
        self.execute_role_based_procedure()
        while True:
            ready_pipes, _, _ = select(
                (self._processor.error_queue._reader, ), (), ()
            )
            handled, msg = self._processor.error_queue.get()
            if handled:
                logger.warning(msg)
            else:
                logger.critical(msg)
                self.execute_role_based_procedure()

    def terminate(self):
        """
        Terminates each component, itself, and the directory observer.
        """
        self._stop_components()
        self._stop_observer()
        if self.is_alive():
            super(Client, self).terminate()

    def _start_components(self):
        """
        Starts the Processor and Listener if the client is not running
        """
        if not self._processor.started:
            try:
                self._processor.exit_queue.get_nowait()
            except Empty:
                pass

            self._processor.start()

        if not self._listener.started:
            try:
                self._listener.exit_queue.get_nowait()
            except Empty:
                pass

            self._listener.start()

    def _stop_components(self):
        """
        Stops the Processor and Listener if the client is running
        """
        if self._listener and not self._listener.cleaned:
            self._listener.terminate()

        if self._processor and not self._processor.cleaned:
            self._processor.terminate()

    def _start_observer(self):
        """
        Schedules the observer using 'settings.WATCH_PATH'
        """
        if self._watch_path:
            logger.info('Starting the directory observer')
            self.directory_observer.schedule(
                self, self._watch_path, recursive=False
            )
            self.directory_observer.start()

    def _stop_observer(self):
        """
        Stops the observer if it is 'alive'
        """
        if self._watch_path:
            logger.info('Stopping the directory observer')
            if self.directory_observer.is_alive():
                self.directory_observer.stop()

    def on_any_event(self, event):
        """
        Listens to an event passed by 'watchdog' and checks the current
        master/slave status

        :arg event: a :class:`~watchdog.events.FileSystemEvent`
        object passed by 'watchdog' indicating an event change within the
        specified directory.
        """
        file_name = event.src_path.split('/')[-1]
        if file_name in self._failover_files:
            self.execute_role_based_procedure()

    def execute_role_based_procedure(self):
        """
        Starts or stops components based on the role (Master/Slave) of the
        Postgres host.

        Implements a binary exponential backoff up to 32 seconds if it
        encounters a FATAL connection error.
        """
        backoff = 0
        while True:
            try:
                server_is_master = self.master_pg_conn.is_server_master()
                if server_is_master:
                    logger.warning(
                        'Server is a master, starting components'
                    )
                    self._start_components()
                else:
                    logger.warning('Server is a slave, stopping components')
                    self._stop_components()
                break
            except OperationalError, e:
                self._stop_components()

                logger.warning(
                    'Cannot connect to the DB: {}'.format(e.pgerror)
                )

                if backoff:
                    backoff <<= 1
                    if backoff >= 32:
                        backoff = 1
                else:
                    backoff = 1
                sleep(backoff)
