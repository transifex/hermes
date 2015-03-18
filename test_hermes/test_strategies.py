from random import choice
from unittest import TestCase

from psycopg2 import InterfaceError, DatabaseError, OperationalError

from hermes.strategies import (
    AbstractErrorStrategy, CommonErrorStrategy, TERMINATE, BACKOFF
)


class AbstractErrorStrategyTestCase(TestCase):
    def test_raises_not_implemented(self):
        strategy = AbstractErrorStrategy()
        self.assertRaises(
            NotImplementedError, strategy.handle_exception, None
        )


class CommonErrorStrategyTestCase(TestCase):
    exception_to_vaue_dict = {
        InterfaceError(): (True, TERMINATE),

        DatabaseError(
            choice(CommonErrorStrategy.BACKOFFABLE_MESSAGE)
        ): (True, BACKOFF),

        OperationalError(): (True, TERMINATE),

        Exception(): (False, TERMINATE)
    }

    def setUp(self):
        self.strat = CommonErrorStrategy()

    def test_strategy_returns_correct_values(self):
        for exception, value in self.exception_to_vaue_dict.iteritems():
            return_value = self.strat.handle_exception(exception)
            self.assertEqual(return_value, value)
