import os
from typing import Optional
from urllib.parse import urlparse

import redis
import redis.asyncio as redis_async


def redis_client(url: Optional[str], socket_timeout: Optional[float] = None, **parameters):
    socket_parameters = {}
    if socket_timeout is not None:
        socket_parameters = {
            "socket_timeout": socket_timeout,
            "socket_connect_timeout": socket_timeout,
            "socket_keepalive": True,
        }
    if url:
        url_parsed = urlparse(url)
        if url_parsed.scheme == "sentinel":
            sentinel_kwargs = {"password": url_parsed.password, **socket_parameters}
            sentinel = redis.Sentinel([(url_parsed.hostname, url_parsed.port)], sentinel_kwargs=sentinel_kwargs)
            return sentinel.master_for(
                service_name=os.path.normpath(url_parsed.path).split("/")[1],
                password=url_parsed.password,
                **socket_parameters,
            )
        else:
            parameters["connection_pool"] = redis.ConnectionPool.from_url(url, **socket_parameters)  # type: ignore
            return redis.Redis(**parameters)


def async_redis_client(url: Optional[str], socket_timeout: Optional[float] = None, **parameters):
    socket_parameters = {}
    if socket_timeout is not None:
        socket_parameters = {
            "socket_timeout": socket_timeout,
            "socket_connect_timeout": socket_timeout,
            "socket_keepalive": True,
        }
    if url:
        url_parsed = urlparse(url)
        if url_parsed.scheme == "sentinel":
            sentinel_kwargs = {"password": url_parsed.password, **socket_parameters}
            sentinel = redis_async.Sentinel([(url_parsed.hostname, url_parsed.port)], sentinel_kwargs=sentinel_kwargs)
            return sentinel.master_for(  # type: ignore
                service_name=os.path.normpath(url_parsed.path).split("/")[1],
                password=url_parsed.password,
                **socket_parameters,
            )
        else:
            parameters["connection_pool"] = redis_async.ConnectionPool.from_url(url, **socket_parameters)
            return redis_async.Redis(**parameters)
