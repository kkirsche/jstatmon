# -*- coding: utf-8 -*-
'''Create a logger instance.'''
# third party packages
from raven import Client, fetch_package_version
from raven.conf import setup_logging
from raven.handlers.logging import SentryHandler

# python stdlib
from logging import basicConfig, getLogger, DEBUG, INFO, ERROR
from logging.handlers import SysLogHandler


def get_sentry_handler():
    '''Setup the Sentry.io handler for catching errors.

    Returns:
        :obj:`raven.handlers.SentryHandler`
    '''
    client = Client(
        dsn=('https://a6c570a9d4b74d7d9e8357af07b7993d:57c715bd3d7448efb7de6f6'
             '677f9abd9@sentry.io/215975'),
        release=fetch_package_version('jstatmon'))
    handler = SentryHandler(client)
    handler.setLevel(ERROR)
    return handler


def setup_syslog_handler(address='/dev/log'):
    """Setup the class logging instance.

    Args:
        address (string or tuple): Where to log syslog information to.
    Returns:
        :obj:`logging.handler.SysLogHandler`
    """
    handler = SysLogHandler(address=address)
    return handler


def setup_logger(log_level=INFO):
    '''Setup the logger with Sentry and Syslog handlers.

    Args:
        log_level (int): The logging level (e.g. logging.INFO)
    Returns:
        :obj:`logging.Logger`
    '''
    log_format = '%(message)s'
    basicConfig(format=log_format)
    handler = get_sentry_handler()
    setup_logging(handler)
    logger = getLogger('jstatmon')
    try:
        logger.addHandler(setup_syslog_handler())
    except Exception:
        logger.error('failed to create syslog hander', exc_info=True)
    logger.setLevel(log_level)
    return logger
