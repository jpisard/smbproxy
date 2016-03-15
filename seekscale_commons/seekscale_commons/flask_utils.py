# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

import json
import traceback
import logging
import locale
from functools import wraps

logger = logging.getLogger(__name__)


def json_response(obj):
    obj['status'] = 'Ok'
    return json.dumps(obj, indent=4)


def json_error(e):
    obj = {
        'status': 'Ko',
        'error': e
    }
    return json.dumps(obj, indent=4)


def json_endpoint(func):
    @wraps(func)
    def inner(*args, **kwargs):
        try:
            ret = func(*args, **kwargs)

            if isinstance(ret, dict):
                return json_response(ret)
            elif isinstance(ret, tuple):
                data, code = ret
                return json_response(data), code
            else:
                return "Configuration error!", 500
        except Exception:
            logger.exception('Exception while processing response')
            exc_string = traceback.format_exc().decode(locale.getpreferredencoding())
            return json_error(exc_string), 500
    return inner
