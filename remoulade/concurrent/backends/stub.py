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
from collections import defaultdict

from ..backend import ConcurrencyBackend
from ..lease import Lease


class StubBackend(ConcurrencyBackend):
    """In-memory backend for tests and single-process runs."""

    def __init__(self):
        self._lock = threading.RLock()
        self._leases: dict[str, dict[str, float]] = defaultdict(dict)

    def acquire(self, *, key: str, limit: int, ttl_ms: int, token: str) -> Lease | None:
        now = time.time() * 1000
        with self._lock:
            self._purge(key, now)
            if len(self._leases[key]) >= limit:
                return None

            expires_at = now + ttl_ms
            self._leases[key][token] = expires_at
            return Lease(key=key, token=token, ttl_ms=ttl_ms, expires_at_ms=int(expires_at))

    def release(self, lease: Lease) -> None:
        with self._lock:
            self._leases.get(lease.key, {}).pop(lease.token, None)

    def _purge(self, key: str, now_ms: float) -> None:
        key_leases = self._leases.get(key)
        if not key_leases:
            return
        expired_tokens = [token for token, expires_at in key_leases.items() if expires_at <= now_ms]
        for token in expired_tokens:
            key_leases.pop(token, None)
