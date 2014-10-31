import random
import string


def rand_string(length):
    choices = string.ascii_uppercase + string.ascii_lowercase + string.digits
    return ''.join(random.choice(choices) for _ in xrange(length))
