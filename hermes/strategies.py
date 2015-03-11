from psycopg2 import InterfaceError, DatabaseError, OperationalError


CONTINUE, BACKOFF, TERMINATE = 1, 2, 3


class AbstractErrorStrategy(object):
    """
    Abstract strategy for handling errors returned from components
    """

    def handle_exception(self, error):
        """
        An abstract method that must be overidden by subclasses.

        Must return a tuple of:
        (Boolean indicating if the exception was expected, a string message)
        """
        raise NotImplementedError("Subclasses MUST override the "
                                  "'handle_exception' method")


class CommonErrorStrategy(AbstractErrorStrategy):
    """
    A common error strategy to deal with Postgres errors
    """

    BACKOFFABLE_MESSAGE = ('teminated abonormally', 'libpq')

    def handle_exception(self, error):
        if isinstance(error, InterfaceError):
            return True, TERMINATE

        elif isinstance(error, (DatabaseError, OperationalError)):
            msg = error.message or error.pgerror
            in_message = False
            if msg:
                in_message = any(m in msg for m in self.BACKOFFABLE_MESSAGE)

            if in_message:
                return True, BACKOFF
            else:
                return True, TERMINATE

        return False, TERMINATE
