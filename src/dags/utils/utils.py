from functools import wraps

import logging
from logging import StreamHandler


def debug(func):
    @wraps(func)
    def wrapper_debug(*args, **kwargs):
        logger = get_logger(logger_name="debug")
        args_repr = [repr(a) for a in args]
        kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
        signature = ", ".join(args_repr + kwargs_repr)
        logger.debug(f"Calling {func.__name__}({signature})")
        value = func(*args, **kwargs)
        logger.debug(f"{func.__name__!r} returned {value!r}")
        return value
    return wrapper_debug


def get_logger(logger_name="super_logger", handler=StreamHandler()):
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    super_logger = logging.getLogger(logger_name)
    super_logger.setLevel(logging.DEBUG)
    handler = handler

    handler.setFormatter(fmt=formatter)
    super_logger.addHandler(handler)
    return super_logger


if __name__ == '__main__':
    pass
