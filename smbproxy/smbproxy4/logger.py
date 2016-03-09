# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

import structlog

ERROR = 5
WARN = 4
INFO = 3
DEBUG = 2


def configure():
    structlog.configure(
        processors=[
            structlog.processors.StackInfoRenderer(),
            structlog.processors.TimeStamper(fmt='iso'),
            structlog.twisted.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.twisted.LoggerFactory(),
        wrapper_class=structlog.twisted.BoundLogger,
        cache_logger_on_first_use=True,
    )

configure()

logger = structlog.getLogger()

plainJSONStdOutLogger = structlog.twisted.plainJSONStdOutLogger
