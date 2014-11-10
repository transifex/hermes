class ErrorStrategy(object):
    """
    Abstract strategy for handling errors returned from components
    """

    def handle_exception(self, error):
        """
        An abstract method that must be overidden by subclasses.

        Must return a tuple of:
        (Boolean indicating if the exception was handled, a string message)
        """
        raise NotImplementedError("Subclasses MUST override the "
                                  "'handle_exception' method")
