class ErrorStrategy(object):
    """
    Abstract strategy for handling errors returned from components
    """

    @classmethod
    def handle_exception(cls, error):
        """
        An abstract method that must be overidden by subclasses.
        """
        raise NotImplementedError("Subclasses MUST override the "
                                  "'handle_exception' method")
