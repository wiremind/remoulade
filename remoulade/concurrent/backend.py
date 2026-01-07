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

from .lease import Lease


class ConcurrencyBackend:
    """Backend interface for distributed concurrency control."""

    def acquire(self, *, key: str, limit: int, ttl_ms: int, token: str) -> Lease | None:
        """Attempt to acquire a concurrency slot for ``key``.

        Returns a :class:`Lease` if a slot is available, otherwise ``None``.
        """

        raise NotImplementedError(f"{type(self).__name__!r} does not implement acquire")

    def release(self, lease: Lease) -> None:
        """Release a previously acquired lease."""

        raise NotImplementedError(f"{type(self).__name__!r} does not implement release")
