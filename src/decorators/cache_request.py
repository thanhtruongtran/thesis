import json as js
from functools import wraps

from sanic import Request, json

from src.constants.time import TimeConstants
from src.services.cached.request_cache import RequestCached
from src.utils.logger import get_logger

logger = get_logger('Cache Request')


def cache(ttl=TimeConstants.MINUTES_5):
    def decorator(f):
        @wraps(f)
        async def decorated_function(request: Request, *args, **kwargs):
            response_data = RequestCached.get_response_data(request)
            if response_data:
                logger.info('Response cached data')
                response = json(response_data)
            else:
                response = await f(request, *args, **kwargs)
                if 200 <= response.status < 300:
                    response_data = js.loads(response.body)
                    RequestCached.cache_response_data(request, response_data, ttl=ttl)
            return response

        return decorated_function

    return decorator
