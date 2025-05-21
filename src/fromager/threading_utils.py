"""Threading utilities for the fromager package."""

import functools
import threading
import typing


def with_thread_lock() -> typing.Callable[[typing.Callable], typing.Callable]:
    """Decorator factory that creates a thread-safe wrapper for a function.

    Each decorated function gets its own lock instance, ensuring that different
    functions using this decorator don't block each other.

    Returns:
        A decorator that wraps the target function with thread-safe execution
    """
    lock = threading.Lock()

    def decorator(func: typing.Callable) -> typing.Callable:
        @functools.wraps(func)
        def wrapper(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            with lock:
                return func(*args, **kwargs)

        return wrapper

    return decorator
