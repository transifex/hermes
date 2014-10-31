# -*- coding: utf-8 -*-

"""
Logging setup for hermes
"""

import logging

from logging.config import dictConfig


LOGGING = {
    'version': 1,
    'formatters': {
        'hermes_stream': {
            'format': '[%(asctime)s %(name)s %(levelname)s] %(message)s',
        },
    },
    'handlers': {
        'log_to_stream': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'hermes_stream',
        },
    },
    'loggers': {
        'hermes': {
            'handlers': ['log_to_stream'],
            'level': 'DEBUG',
            'propagate': False,
        }
    }
}

dictConfig(LOGGING)
logger = logging.getLogger('hermes')
