# This file is a part of Remoulade.
#
# Copyright (C) 2017,2018 CLEARTYPE SRL <bogdan@cleartype.io>
#
# Remoulade is free software; you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at
# your option) any later version.
#
# Remoulade is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public
# License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
from limits import RateLimitItem
from limits.util import parse_many

from ..logging import get_logger
from ..middleware import Middleware
from .backend import RateLimitBackend
from .errors import RateLimitExceeded, RateLimitSpecificationError


class _RateLimit(Middleware):
    """Abstract middleware enforcing rate limits

    Parameters:
      backend(RateLimitMiddlewareBackend): Backend used to keep rate limit counters.
    """

    def __init__(self, *, backend: RateLimitBackend):
        if backend is None:
            raise ValueError("backend is required for RateLimit middleware")

        self.backend = backend
        self.logger = get_logger(__name__, type(self))
        self._limits_cache: dict[str, list[RateLimitItem]] = {}

    @property
    def actor_options(self) -> set[str]:
        return {"rate_limits", "bypass_rate_limits"}

    def _parse_limits(self, spec: str) -> list[RateLimitItem]:
        if spec not in self._limits_cache:
            try:
                self._limits_cache[spec] = list(parse_many(spec))
            except ValueError as exc:
                raise RateLimitSpecificationError(f"Invalid rate_limits specification: {spec!r}") from exc
        return self._limits_cache[spec]

    def _check_limits(self, broker, message, option_name):
        spec = self.get_option(option_name, broker=broker, message=message)
        if not spec:
            return

        bypass_rate_limits = bool(self.get_option("bypass_rate_limits", broker=broker, message=message))

        limits = self._parse_limits(spec)
        key = message.actor_name

        for limit in limits:
            if not self.backend.hit(limit, key):
                self.logger.debug("Rate limit exceeded for key %r limit %r", key, limit)
                if not bypass_rate_limits:
                    raise RateLimitExceeded(f"rate limit exceeded for key {key!r}")


class RateLimitEnqueue(_RateLimit):
    """Middleware enforcing rate limits at enqueue time.

    Parameters:
      backend(RateLimitBackend): Backend used to keep rate limit counters.
    """

    @property
    def actor_options(self) -> set[str]:
        return {"enqueue_rate_limits", "bypass_rate_limits"}

    def before_enqueue(self, broker, message, delay):
        self._check_limits(broker, message, "enqueue_rate_limits")


class RateLimitProcess(_RateLimit):
    """Middleware enforcing rate limits at process time.

    Parameters:
      backend(RateLimitBackend): Backend used to keep rate limit counters.
    """

    @property
    def actor_options(self) -> set[str]:
        return {"process_rate_limits", "bypass_rate_limits"}

    def before_process_message(self, broker, message):
        self._check_limits(broker, message, "process_rate_limits")
