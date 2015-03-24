# -*- coding: utf-8 -*-
from Queue import Empty
from multiprocessing.process import Process
from multiprocessing.queues import Queue
import select
from signal import signal, SIGCHLD, SIGINT, SIGTERM
from time import sleep
import os

from psycopg2 import OperationalError
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from hermes.components import Component
from hermes.connectors import PostgresConnector
from hermes.exceptions import InvalidConfigurationException
from hermes.log import LoggerMixin
from hermes.strategies import TERMINATE


class Client(LoggerMixin, Process, FileSystemEventHandler):
    """
    Responsible for Listener and Processor components. Provides
    functions to start/stop both itself and its components. In addition, it is
    also capable of receiving file-system events via the 'watchdog' library.

    General procedure:

        1. Starts both the Process and Listener components.
        2. Listen and act upon exit/error notifications from components
        3. Listen for file-system events and acts accordingly.
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

        :param dsn: A Postgres-compatible DSN dictionary
        :param watch_path: The directory to monitor for filechanges. If None,
            then file monitoring is disabled.
        :param failover_files: A list of files which, when modified, will
            cause the client to call :func:`~execute_role_based_procedure`
        """
        super(Client, self).__init__()

        self.directory_observer = Observer()

        self._processor = None
        self._listener = None

        self._watch_path = watch_path
        self._failover_files = failover_files
        self.master_pg_conn = PostgresConnector(dsn)

        self._should_run = False
        self._child_interrupted = False
        self._exception_raised = False

        self._exit_queue = Queue(1)

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
        signal(SIGINT, self._handle_terminate)
        signal(SIGTERM, self._handle_terminate)
        self._validate_components()
        super(Client, self).start()

    def run(self):
        """
        Performs a :func:`~select.select` on the components' error queue.
        When a notification is detected, the client will log the message and
        then calculate if the Postgres server is still a Master -  if not, the
        components are shutdown.
        """
        super(Client, self).run()
        self._start_observer()

        signal(SIGCHLD, self._handle_sigchld)

        self._should_run = True

        self.execute_role_based_procedure()
        while self._should_run:
            self._exception_raised = self._child_interrupted = False
            try:
                exit_pipe = self._exit_queue._reader

                ready_pipes, _, _ = select.select(
                    (exit_pipe, ), (), ()
                )

                if exit_pipe in ready_pipes:
                    self.terminate()

            except select.error:
                if not self._child_interrupted and not self._exception_raised:
                    self._should_run = False

    def _start_components(self, restart=False):
        """
        Starts the Processor and Listener if the client is not running
        """
        if not self._processor.is_alive():
            if restart and self._processor.ident:
                self._processor.join()
            self._processor.start()

        if not self._listener.is_alive():
            if restart and self._listener.ident:
                self._listener.join()
            self._listener.start()

    def _stop_components(self):
        """
        Stops the Processor and Listener if the client is running
        """
        if self._listener and self._listener.ident and self._listener.is_alive():
            self._listener.terminate()
            self._listener.join()

        if self._processor and self._processor.ident and self._processor.is_alive():
            self._processor.terminate()
            self._processor.join()

    def _start_observer(self):
        """
        Schedules the observer using 'settings.WATCH_PATH'
        """
        if self._watch_path:
            self.directory_observer.schedule(
                self, self._watch_path, recursive=False
            )
            self.directory_observer.start()

    def _stop_observer(self):
        """
        Stops the observer if it is 'alive'
        """
        if self._watch_path and self.directory_observer:
            if self.directory_observer.is_alive():
                self.directory_observer.stop()

    def on_any_event(self, event):
        """
        Listens to an event passed by 'watchdog' and checks the current
        master/slave status

        :param event: A :class:`~watchdog.events.FileSystemEvent`
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

        Implements a `binary exponential backoff
        <http://en.wikipedia.org/wiki/Exponential_backoff
        #Binary_exponential_backoff_.2F_truncated_exponential_backoff>`_
        up to 32 seconds if it encounters a FATAL connection error.
        """
        backoff = 0
        while True:
            try:
                server_is_master = self.master_pg_conn.is_server_master()
                if server_is_master:
                    self.log.warning('Server is a master, starting components')
                    self._start_components(restart=True)
                else:
                    self.log.warning('Server is a slave, stopping components')
                    self._stop_components()
                break
            except OperationalError:
                self._stop_components()

                self.log.warning(
                    'Cannot connect to the DB, maybe it has been shutdown?',
                    exc_info=True
                )

                if backoff:  # pragma: no cover
                    backoff <<= 1
                    if backoff > 32:
                        backoff = 1
                else:
                    backoff = 1
                sleep(backoff)

    def _handle_sigchld(self, sig, frame):
        """
        A child process dying, and the client not shutting down, indicates
        a process has been shut down by some external caller.

        We must check both the processor and listener for 'liveness' and
        start those which have failed.
        """
        if sig == SIGCHLD and self._should_run and not self._exception_raised:
            try:
                expected, action = self._processor.error_queue.get_nowait()
                self._exception_raised = True
                if expected:
                    if action == TERMINATE:
                        self.execute_role_based_procedure()
                else:
                    self.log.critical(
                        'An unexpected error was raised - shutting down'
                    )
                    self._shutdown()
            except Empty:
                self._child_interrupted = True
                self._start_components(restart=True)

    def _handle_terminate(self, sig, frame):
        """
        Handles SIGINT and SIGTERM signals.

        If called from another process then puts to the exit queue, else
        calls _shutdown.
        """
        if self.ident != os.getpid():
            self._exit_queue.put_nowait(True)
        else:
            self._shutdown()

    def _shutdown(self):
        """
        Shuts down the Client:
            * Sets '_should_run' to False.
            * Stops the components.
            * Stops the observer.
        """
        self.log.warning('Shutting down...')
        self._should_run = False
        self._stop_components()
        self._stop_observer()
