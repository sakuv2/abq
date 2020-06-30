import asyncio
import logging
from functools import wraps

logger = logging.getLogger(__name__)


class TooManyTriesException(BaseException):
    pass


def retry(times, exception=Exception):
    def func_wrapper(f):
        @wraps(f)
        async def wrapper(*args, **kwargs):
            exc = None
            for time in range(times):
                # noinspection PyBroadException
                try:
                    return await f(*args, **kwargs)
                except exception as e:
                    logger.warning(f"{type(e)}: {e.args[0]}")
                    exc = e
                logger.debug(f"retry times: {time+1} in {f.__name__}()")
                await asyncio.sleep(0.1)
            raise TooManyTriesException() from exc

        return wrapper

    return func_wrapper
