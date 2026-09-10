# Fabric notebook source


# CELL ********************

import logging
import logging.config
from contextvars import ContextVar

# Context variable to store the request ID
request_id_var = ContextVar('request_id', default=None)

class ContextFilter(logging.Filter):
    def filter(self, record):
        # Retrieve the request ID from the context variable and attach it to the log record
        record.request_id = request_id_var.get() or "N/A"
        return True


import logging
import logging.config

def setup_logging(default_level=logging.INFO):
    """
    Set up logging configuration
    """
    logging_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'standard',
                'level': logging.DEBUG,
            },
        },
        'root': {
            'handlers': ['console'],
            'level': default_level,
        },
        'loggers': {
            '': {
                'handlers': ['console'],
                'level': default_level,
                'propagate': True
            },
        }
    }

    logging.config.dictConfig(logging_config)


def setup_logging(default_level=logging.INFO):
    logging_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - [%(request_id)s] - %(message)s',
            },
        },
        'filters': {
            'context_filter': {
                '()': ContextFilter,
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'standard',
                'filters': ['context_filter'],
                'level': logging.DEBUG,
            },
        },
        'root': {
            'handlers': ['console'],
            'level': default_level,
            'filters': ['context_filter'],
        },
        'loggers': {
            '': {
                'handlers': ['console'],
                'level': default_level,
                'propagate': True
            },
        }
    }

    logging.config.dictConfig(logging_config)

