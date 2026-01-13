# This file is a part of Remoulade.
#
# Copyright (C) 2017,2018 WIREMIND SAS <dev@wiremind.fr>
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

import threading
import time

from ..logging import get_logger
from ..middleware import Middleware
from .backend import ConcurrencyBackend
from .errors import ConcurrencyLimitExceeded
from .lease import Lease

DEFAULT_TTL_MS = 5 * 60 * 1000


class Concurrent(Middleware):
    """Middleware enforcing distributed concurrency limits before processing."""

    def __init__(
        self,
        *,
        backend: ConcurrencyBackend,
        default_ttl_ms: int = DEFAULT_TTL_MS,
    ):
        self.backend = backend
        self.default_ttl_ms = default_ttl_ms
        self.logger = get_logger(__name__, type(self))
        self._leases: dict[str, list[Lease]] = {}
        self._lock = threading.RLock()

    @property
    def actor_options(self) -> set[str]:
        return {
            "bypass_concurrency_limits",
            "concurrency_key",
            "concurrency_limit",
            "concurrency_ttl",
        }

    def before_process_message(self, broker, message):
        limit = self.get_option("concurrency_limit", broker=broker, message=message)
        if not limit:
            return

        if limit < 1:
            raise ValueError("concurrency_limit must be positive")

        ttl_ms = self.get_option("concurrency_ttl", broker=broker, message=message) or self.default_ttl_ms
        key_option = self.get_option("concurrency_key", broker=broker, message=message)
        key = self._resolve_key(key_option, message)
        lease = self.backend.acquire(key=key, limit=int(limit), ttl_ms=int(ttl_ms), token=message.message_id)

        if lease is None:
            if self.get_option("bypass_concurrency_limits", broker=broker, message=message):
                self.logger.debug("Bypassing concurrency limit for key %r", key)
                return
            raise ConcurrencyLimitExceeded(f"concurrency limit exceeded for key {key!r}")

        self.logger.debug("Acquired concurrency lease for key %r", key)
        self._store_lease(message.message_id, lease)

    def after_process_message(self, broker, message, *, result=None, exception=None):
        self._release_leases(message)

    def after_skip_message(self, broker, message):
        self._release_leases(message)

    def after_message_canceled(self, broker, message):
        self._release_leases(message)

    def _release_leases(self, message):
        leases = self._pop_leases(message.message_id)
        for lease in leases:
            try:
                self.backend.release(lease)
                self.logger.debug("Released concurrency lease for key %r", lease.key)
            except Exception as exc:  # pragma: no cover - defensive
                self.logger.warning("Failed to release concurrency lease for key %r: %s", lease.key, exc)

    def _resolve_key(self, key_option: str | None, message) -> str:
        if key_option is None:
            return message.actor_name

        try:
            return key_option.format(
                actor_name=message.actor_name,
                args=message.args,
                kwargs=message.kwargs,
            )
        except Exception:
            return key_option

    def _store_lease(self, message_id: str, lease: Lease) -> None:
        with self._lock:
            self._purge_leases()
            self._leases.setdefault(message_id, []).append(lease)

    def _pop_leases(self, message_id: str) -> list[Lease]:
        with self._lock:
            return self._leases.pop(message_id, [])

    def _purge_leases(self) -> None:
        # We keep a buffer of DEFAULT_TTL_MS to avoid purging leases that are about to expire or should have expired not
        # that long ago
        now_with_buffer = (time.time() * 1000) - DEFAULT_TTL_MS
        for message_id in list(self._leases.keys()):
            leases = [lease for lease in self._leases[message_id] if lease.expires_at_ms > now_with_buffer]
            if leases:
                self._leases[message_id] = leases
            else:
                self._leases.pop(message_id, None)
