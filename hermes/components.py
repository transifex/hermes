from Queue import Full, Empty
from errno import EINTR
from multiprocessing import Process
import select

from psycopg2._psycopg import InterfaceError

from hermes.log import logger


class Component(Process):
    def __init__(self, notification_pipe, error_strategy,
                 exit_queue, error_queue, pg_connector):
        self.error_strategy = error_strategy
        self.exit_queue = exit_queue
        self.error_queue = error_queue
        self.pg_connector = pg_connector
        self.notification_pipe = notification_pipe
        self.cleaned = False
        self.started = False

    def execute(self):
        raise NotImplementedError(
            "Subclasses MUST override the 'execute' method"
        )

    def execution_done(self):
        raise NotImplementedError(
            "Subclasses MUST override the 'execution_done' method"
        )

    def start(self):
        if not self.started:
            self.__empty_exit_queue__()
            super(Component, self).__init__()
            self.cleaned = False
            self.started = True
            super(Component, self).start()

    def __empty_exit_queue__(self):
        try:
            while True:
                self.exit_queue.get_nowait()
        except Empty:
            pass

    def run(self):
        """

        """
        while True:
            try:
                ready_pipes, _, _ = select.select(
                    (self.notification_pipe, self.exit_queue._reader), [], []
                )
            except select.error, ex:
                # If we get an EINTR(4) exception then we should break out
                # because the client is shutting down and interrupts will be
                # thrown around like it's some sort of interrupt party and
                # everyone is invited.
                if ex[0] == EINTR:
                    break
                else:
                    self.cleanup()
                    raise
            except InterfaceError:
                # The Database is shutting or has shut down. We can safely
                # break out of this component and we'll be called later on.
                break

            if self.exit_queue._reader in ready_pipes:
                logger.debug('{} shutting down'.format(
                    self.__class__.__name__
                ))
                break

            try:
                if self.notification_pipe in ready_pipes:
                    logger.debug(
                        '{} received notification, running "execute"'.format(
                            self.__class__.__name__
                        )
                    )
                    self.execute()
                    self.execution_done()
            except Exception, e:
                handled, message = self.error_strategy.handle_exception(e)
                self.error_queue.put((handled, message))
        self.cleanup()

    def join(self, timeout=None):
        """
        Joins the component if 'self.started' is set to True. Calls
        'self.cleanup' regardless of outcome.
        """
        if self.started:
            super(Component, self).join(timeout=timeout)
        self.cleanup()

    def cleanup(self):
        """
        Disconnects the attached connector and sets 'self.cleaned' to True.
        """
        if self.cleaned:
            return
        self.pg_connector.disconnect()
        self.cleaned = True
        self.started = False

    def terminate(self):
        """
        Terminates the component by putting an entry on the 'exit_queue' and
        joining the current thread.
        """
        try:
            self.exit_queue.put_nowait(True)
        except Full:
            pass
        self.join()
