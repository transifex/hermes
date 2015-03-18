from multiprocessing import Process
import select
from signal import signal, SIGTERM, SIGINT
from time import sleep

from hermes.log import LoggerMixin
from hermes import strategies


_LOG_PID = 'Started with PID:{}'
_HANDLED_EXCEPTION = 'Handled an exception'
_TERMINATED_ON_EXCEPTION = 'Terminated due to exception'
_UNHANDLED_EXCEPTION = 'An unhandled exception has been raised'
_BACKOFF_EXCEPTION = 'Backing off {} seconds due to an exception'


class Component(LoggerMixin, Process):
    """
    A class which can be used to create both listener and processor
    objects. Callers must implement :func:`~execute` and can others if
    they so choose.

    The structure of calls::

        +-----------------+
        |      set_up     | <--------------+
        +-----------------+                |
                 +                         |
                 |                         |
                 |                         |   ERROR
                 V                         |
      +-----------------------+            |
      |      pre_execute      |            |
      |        execute        | +----------+
      |      post_execute     |
      +-----------------------+
                 +
                 |
                 |
                 V
      +-----------------------+
      |      tear_down        |
      +-----------------------+
    """

    def __init__(self, notification_pipe, error_strategy,
                 error_queue, backoff_limit=16):
        """
        The Component class adds a foundation for you to build a
        fully-fledged processor or listener. You can add/modify as much
        as you like - sensitive methods have been identified.

        :param notification_pipe: The :class:`~multiprocessing.Pipe`-like
            object to perform :func:`~select.select` on.
        :param error_strategy: An object of type
            :class:`~hermes.strategies.AbstractErrorStrategy` to handle
            exceptions.
        :param error_queue: A :class:`~multiprocessing.Queue`-like object
            to inform the :class:`~hermes.client.Client` through.
        :param backoff_limit: The maximum number of seconds to backoff a
            Component until it resets.
        """
        self.error_strategy = error_strategy
        self.error_queue = error_queue
        self.notification_pipe = notification_pipe

        self._should_run = False
        self._backoff_limit = backoff_limit

        self.__backoff_time__ = 0

    def pre_execute(self):
        """
        Can be safely overridden by callers. The return value will be
        passed to :func:`~execute`.
        """
        pass  # pragma: no cover

    def execute(self, pre_exec_value):
        """
        Must be overridden by callers. The return value will be
        passed to :func:`~post_execute`

        :param pre_exec_value: The value returned by :func:`~pre_execute`
        """
        raise NotImplementedError(
            "Subclasses MUST override the 'execute' method"
        )

    def post_execute(self, exec_value):
        """
        Can be safely overridden by callers.

        :param exec_value: The value returned by :func:`~execute`
        """
        pass  # pragma: no cover

    def set_up(self):
        """
        Called before execute methods and only once per iteration.

        Overridden methods should call super.
        """
        signal(SIGTERM, self._handle_stop_signal)
        signal(SIGINT, self._handle_stop_signal)

    def tear_down(self):
        """
        Called after execute methods and only once per iteration.

        Can be used to tear down any resources.
        """
        pass  # pragma: no cover

    def start(self):
        """
        Initialises the process, sets it to daemonic and starts.
        """
        super(Component, self).__init__()
        self.daemon = True
        super(Component, self).start()

    def run(self):
        """
        The main Component loop.

        Callers should take great care when overriding.
        """
        super(Component, self).run()
        self.log.debug(_LOG_PID.format(self.pid))
        self._should_run = True
        while self._should_run:
            try:
                self.set_up()
                self._execute()
            except select.error:
                break
            except Exception, e:
                expected, action = self.error_strategy.handle_exception(e)
                if action == strategies.CONTINUE:
                    self.log.warning(_HANDLED_EXCEPTION, exc_info=True)
                    continue
                elif action == strategies.BACKOFF:
                    self._backoff()
                    continue
                elif expected and action == strategies.TERMINATE:
                    self.log.warning(_TERMINATED_ON_EXCEPTION, exc_info=True)
                else:
                    self.log.critical(_UNHANDLED_EXCEPTION, exc_info=True)

                self.error_queue.put((expected, action))
                self._should_run = False
            finally:
                self.tear_down()

    def _execute(self):
        """
        Loops through select -> post_exec(execute(pre_execute)) until
        terminate is called or an exception is raised.
        """
        while self._should_run:
            ready_pipes, _, _ = select.select(
                (self.notification_pipe, ), (), ()
            )

            if self.notification_pipe in ready_pipes:
                self.log.debug('Received notification, running execute')
                self.post_execute(self.execute(self.pre_execute()))

        self.__backoff_time__ = 0

    def is_alive(self):
        """
        :return: :func:`~Process.is_alive` unless the Component has
            not been started and returns False.
        """
        try:
            return super(Component, self).is_alive()
        except AttributeError:
            return False

    def join(self, **kwargs):
        """
        :return: :func:`~Process.join` unless the Component has not
            been started and returns immediately.
        """
        try:
            return super(Component, self).join(**kwargs)
        except AttributeError:
            return

    @property
    def ident(self):
        """
        :return: :func:`~multiprocessing.Process.ident` unless the
            Component has not been started and returns None.
        """
        try:
            return super(Component, self).ident
        except AttributeError:
            return None

    def _backoff(self):
        """
        Backs off up to the provided backoff_limit.
        """
        msg = _BACKOFF_EXCEPTION.format(self.__backoff_time__)
        self.log.warning(msg, exc_info=True)

        if self.__backoff_time__:
            self.__backoff_time__ <<= 1
            if self.__backoff_time__ > self._backoff_limit:
                self.__backoff_time__ = 1
        else:
            self.__backoff_time__ = 1

        sleep(self.__backoff_time__)
        self.log.warning('Retrying...')

    def _handle_stop_signal(self, sig, frame):
        """
        Terminates the Component by setting the _should_run
        flag to False.
        """
        self._should_run = False
