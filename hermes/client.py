# -*- coding: utf-8 -*-
from Queue import Empty
from multiprocessing.process import Process
from select import select
from time import sleep

from psycopg2._psycopg import OperationalError
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from components import Component
from hermes.connectors import PostgresConnector
from log import logger


class HermesClient(Process, FileSystemEventHandler):
    """
    A client to hold references to the Processor and Listener components
    associated with a River.

    In addition, it is also capable of receiving file-system events via the
    'watchdog' library.

    General procedure:
        * Starts both the Process and Listener components.
        * Listen and act upon exit/error notifications from components
        * Listen for file-system events and act accordingly.
    """

    def __init__(self, dsn, watch_path, failover_files):
        super(HermesClient, self).__init__()

        self.directory_observer = Observer()

        self._processor = None
        self._listener = None

        self._started = False
        self._should_run = True

        self._watch_path = watch_path
        self._failover_files = failover_files
        self.master_pg_conn = PostgresConnector(dsn)

    def add_processor(self, processor):
        assert isinstance(processor, Component), \
            "Processor must be of type Component"
        self._processor = processor

    def add_listener(self, listener):
        assert isinstance(listener, Component), \
            "Listener must of type Component"
        self._listener = listener

    def _validate_components(self):
        assert self._processor, "A processor must be defined"
        assert self._listener, "A listener must be defined"
        assert self._processor.error_queue is self._listener.error_queue

    def start(self):
        """
        Starts itself, its components and its directory observer.
        """
        self._validate_components()
        self.execute_role_based_procedure()
        while self._should_run:
            ready_pipes, _, _ = select(
                (self._processor.error_queue._reader, ), (), ()
            )
            handled, msg = self._processor.error_queue.get()
            if handled:
                logger.warning(msg)
            else:
                logger.critical(msg)
                self.execute_role_based_procedure()
        if not self._started:
            self.start_observer()

    def run(self):
        """
        Listens to errors reported by components and queries whether the
        current server is a Master or Slave.
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
        Terminates each component, itself, and sets the 'running' flag to
        false
        """
        self.stop_components()
        self.stop_observer()
        if self.is_alive():
            super(HermesClient, self).terminate()

    def start_components(self):
        """
        Starts the Processor and Listener if the client is not running
        """
        if self._processor.cleaned:
            try:
                self._processor.exit_queue.get_nowait()
            except Empty:
                pass

            self._processor.start()

        if self._listener.cleaned:
            try:
                self._listener.exit_queue.get_nowait()
            except Empty:
                pass

            self._listener.start()

    def stop_components(self):
        """
        Stops the Processor and Listener if the client is running
        """
        if self._listener and not self._listener.cleaned:
            self._listener.terminate()

        if self._processor and not self._processor.cleaned:
            self._processor.terminate()

    def start_observer(self):
        """
        Schedules the observer using 'settings.WATCH_PATH'
        """
        logger.info('Starting the directory observer')
        self.directory_observer.schedule(
            self, self._watch_path, recursive=False
        )
        self.directory_observer.start()

    def stop_observer(self):
        """
        Stops the observer if it is 'alive'
        """
        logger.info('Stopping the directory observer')
        if self.directory_observer.is_alive():
            self.directory_observer.stop()

    def on_any_event(self, event):
        """
        Listens to any event passed by 'watchdog' and checks the current
        master/slave status

        :param event: an event object passed by 'watchdog'
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
                    self.start_components()
                else:
                    logger.warning('Server is a slave, stopping components')
                    self.stop_components()
                break
            except OperationalError, e:
                self.stop_components()

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
