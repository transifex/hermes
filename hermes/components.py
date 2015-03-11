from multiprocessing import Process
import select
from signal import signal, SIGTERM, SIGINT
from time import sleep

from hermes.log import LoggerMixin
from hermes import strategies


_LOG_PID_AND_MODULE_NAME = '"{}" started with PID:{}'

_HANDLED_EXCEPTION = 'Handled an exception'
_TERMINATED_ON_EXCEPTION = 'Terminated due to exception'
_UNHANDLED_EXCEPTION = 'An unhandled exception has been raised'
_BACKOFF_EXCEPTION = 'Backing off {} due to an exception'


class Component(LoggerMixin, Process):
    def __init__(self, notification_pipe, error_strategy,
                 error_queue, backoff_limit=3):
        self.error_strategy = error_strategy
        self.error_queue = error_queue
        self.notification_pipe = notification_pipe

        self._should_run = False
        self._backoff_limit = backoff_limit

    def pre_execute(self):
        pass

    def execute(self, pre_exec_value):
        raise NotImplementedError(
            "Subclasses MUST override the 'execute' method"
        )

    def post_execute(self, exec_value):
        pass

    def set_up(self):
        """

        :return:
        """
        signal(SIGTERM, self._handle_stop_signal)
        signal(SIGINT, self._handle_stop_signal)

    def tear_down(self):
        """

        :return:
        """
        pass

    def start(self):
        super(Component, self).__init__()
        self.daemon = True
        super(Component, self).start()

    def run(self):
        """

        :return:
        """
        super(Component, self).run()
        self.log.debug(_LOG_PID_AND_MODULE_NAME.format(self.name, self.pid))
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
                elif action == strategies.TERMINATE:
                    self.log.warning(_TERMINATED_ON_EXCEPTION, exc_info=True)
                elif action == strategies.BACKOFF:
                    self._backoff()
                    continue
                else:
                    self.log.critical(_UNHANDLED_EXCEPTION, exc_info=True)

                self.error_queue.put((expected, action))
                self._should_run = False
            finally:
                self.tear_down()

    def _execute(self):
        """

        :return:
        """
        while self._should_run:
            ready_pipes, _, _ = select.select(
                (self.notification_pipe, ), (), ()
            )

            if self.notification_pipe in ready_pipes:
                self.log.debug('Received notification, running execute')
                self.post_execute(self.execute(self.pre_execute()))

    def is_alive(self):
        """

        :return:
        """
        try:
            return super(Component, self).is_alive()
        except AttributeError:
            return False

    def join(self, **kwargs):
        """

        :param kwargs:
        :return:
        """
        try:
            return super(Component, self).join(**kwargs)
        except AttributeError:
            return

    @property
    def ident(self):
        """

        :return:
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
                self.__backoff_time__ = 0
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
