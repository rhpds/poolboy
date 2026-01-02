"""Timer decorators for automatic method execution timing."""

import inspect
import os
import time
from functools import wraps

from .app_metrics import AppMetrics

CLUSTER_DOMAIN = os.environ.get('CLUSTER_DOMAIN', 'unknown')


def async_timer(app: str):
    """Decorator that records execution time of async functions."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            status = 'success'
            try:
                result = await func(*args, **kwargs)
                return result
            except Exception as e:
                status = 'error'
                raise e
            finally:
                duration = time.time() - start_time
                AppMetrics.process_time.labels(
                    method=func.__name__,
                    status=status,
                    app=app,
                    cluster_domain=CLUSTER_DOMAIN,
                ).observe(duration)
        return wrapper
    return decorator


def sync_timer(app: str):
    """Decorator that records execution time of sync functions."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            status = 'success'
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                status = 'error'
                raise e
            finally:
                duration = time.time() - start_time
                AppMetrics.process_time.labels(
                    method=func.__name__,
                    status=status,
                    app=app,
                    cluster_domain=CLUSTER_DOMAIN,
                ).observe(duration)
        return wrapper
    return decorator


class TimerDecoratorMeta(type):
    """Metaclass that applies timer decorators to all public methods."""

    def __new__(cls, name, bases, dct):
        for attr_name, attr_value in dct.items():
            if isinstance(attr_value, staticmethod):
                original_method = attr_value.__func__
                if inspect.iscoroutinefunction(original_method):
                    decorated_method = async_timer(name)(original_method)
                else:
                    decorated_method = sync_timer(name)(original_method)
                dct[attr_name] = staticmethod(decorated_method)
            elif isinstance(attr_value, classmethod):
                original_method = attr_value.__func__
                if inspect.iscoroutinefunction(original_method):
                    decorated_method = async_timer(name)(original_method)
                else:
                    decorated_method = sync_timer(name)(original_method)
                dct[attr_name] = classmethod(decorated_method)
            elif callable(attr_value) and not attr_name.startswith("__"):
                if inspect.iscoroutinefunction(attr_value):
                    dct[attr_name] = async_timer(name)(attr_value)
                else:
                    dct[attr_name] = sync_timer(name)(attr_value)
        return super().__new__(cls, name, bases, dct)
