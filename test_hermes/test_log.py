from unittest import TestCase
from hermes import log as hermes_log
from multiprocessing import current_process


class LogTestCase(TestCase):
    def test_get_logger_creates_logger_and_adds_handler_only_once(self):
        logger = hermes_log.get_logger(current_process())

        self.assertIsNotNone(logger)
        self.assertEqual(len(logger.handlers), 1)

        logger = hermes_log.get_logger(current_process())
        self.assertEqual(len(logger.handlers), 1)
