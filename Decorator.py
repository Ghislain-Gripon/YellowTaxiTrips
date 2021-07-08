import logging, functools

def logging_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if args is None:
                args = ''
            if kwargs is None:
                kwargs = ''
            logging.debug('{} run with {} {} arguments.'.format(func.__name__, ' '.join(args), ','+' '.join(kwargs)))
            val = func(*args, **kwargs)
            logging.debug('{} returned {} .'.format( func.__name__, val))
            return val
        return wrapper
