import random
import string


def rand_string(length):
    choices = string.ascii_uppercase + string.ascii_lowercase + string.digits
    return ''.join(random.choice(choices) for _ in xrange(length))


class LimitedTrueBool(int):
    def __init__(self, x):
        super(LimitedTrueBool, self).__init__(x)
        self.called_no = 0

    def __nonzero__(self):
        try:
            if self.called_no < self:
                self.called = True
                return True
            else:
                return False
        finally:
            self.called_no += 1