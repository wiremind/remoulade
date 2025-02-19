import os
from typing import Optional
from urllib.parse import urlparse

import redis
import redis.asyncio as redis_async


def redis_client(url: Optional[str], **parameters):
    """"""
    if url:
        url_parsed = urlparse(url)
        if url_parsed.scheme == "sentinel":
            sentinel = redis.Sentinel(
                [(url_parsed.hostname, url_parsed.port)],
                sentinel_kwargs={"password": url_parsed.password},
            )
            return sentinel.master_for(
                service_name=os.path.normpath(url_parsed.path).split("/")[1], password=url_parsed.password
            )
        else:
            parameters["connection_pool"] = redis.ConnectionPool.from_url(url)
            return redis.Redis(**parameters)


def async_redis_client(url: Optional[str], **parameters):
    """"""
    if url:
        url_parsed = urlparse(url)
        if url_parsed.scheme == "sentinel":
            sentinel = redis_async.Sentinel(
                [(url_parsed.hostname, url_parsed.port)],
                sentinel_kwargs={"password": url_parsed.password},
            )
            return sentinel.master_for(
                service_name=os.path.normpath(url_parsed.path).split("/")[1], password=url_parsed.password
            )
        else:
            parameters["connection_pool"] = redis_async.ConnectionPool.from_url(url)
            return redis_async.Redis(**parameters)
