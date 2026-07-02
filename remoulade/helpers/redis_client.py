import os
from urllib.parse import urlparse

import redis
import redis.asyncio as redis_async
from redis.asyncio.retry import Retry as AsyncRetry
from redis.backoff import ExponentialBackoff
from redis.retry import Retry

HEALTH_CHECK_INTERVAL = 30
CONNECTION_MAX_RETRIES = 3


def _connection_parameters(retry, socket_timeout):
    # health_check_interval re-pings an idle connection and reconnects it before reuse; retry + retry_on_error
    # transparently re-run a command over a fresh connection on a reset or timeout, which also recovers Sentinel
    # master rediscovery since MasterNotFoundError is a ConnectionError.
    parameters = {
        "socket_keepalive": True,
        "health_check_interval": HEALTH_CHECK_INTERVAL,
        "retry": retry,
        "retry_on_error": [redis.ConnectionError, redis.TimeoutError],
    }
    if socket_timeout is not None:
        parameters["socket_timeout"] = socket_timeout
        parameters["socket_connect_timeout"] = socket_timeout
    return parameters


def redis_client(url: str | None, socket_timeout: float | None = None, **parameters):
    connection_parameters = _connection_parameters(Retry(ExponentialBackoff(), CONNECTION_MAX_RETRIES), socket_timeout)
    if url:
        url_parsed = urlparse(url)
        if url_parsed.scheme == "sentinel":
            sentinel_kwargs = {"password": url_parsed.password, **connection_parameters}
            sentinel = redis.Sentinel([(url_parsed.hostname, url_parsed.port)], sentinel_kwargs=sentinel_kwargs)
            return sentinel.master_for(
                service_name=os.path.normpath(url_parsed.path).split("/")[1],
                password=url_parsed.password,
                **connection_parameters,
            )
        else:
            parameters["connection_pool"] = redis.ConnectionPool.from_url(url, **connection_parameters)
            return redis.Redis(**parameters)


def async_redis_client(url: str | None, socket_timeout: float | None = None, **parameters):
    connection_parameters = _connection_parameters(
        AsyncRetry(ExponentialBackoff(), CONNECTION_MAX_RETRIES), socket_timeout
    )
    if url:
        url_parsed = urlparse(url)
        if url_parsed.scheme == "sentinel":
            sentinel_kwargs = {"password": url_parsed.password, **connection_parameters}
            sentinel = redis_async.Sentinel([(url_parsed.hostname, url_parsed.port)], sentinel_kwargs=sentinel_kwargs)
            return sentinel.master_for(
                service_name=os.path.normpath(url_parsed.path).split("/")[1],
                password=url_parsed.password,
                **connection_parameters,
            )
        else:
            parameters["connection_pool"] = redis_async.ConnectionPool.from_url(url, **connection_parameters)
            return redis_async.Redis(**parameters)
