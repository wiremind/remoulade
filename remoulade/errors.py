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


class RemouladeError(Exception):  # pragma: no cover
    """Base class for all remoulade errors."""

    def __init__(self, message: str) -> None:
        self.message = message

    def __str__(self) -> str:
        return str(self.message) or repr(self.message)


class BrokerError(RemouladeError):
    """Base class for broker-related errors."""


class ActorNotFound(BrokerError):
    """Raised when a message is sent to an actor that hasn't been declared."""


class QueueNotFound(BrokerError):
    """Raised when a message is sent to an queue that hasn't been declared."""


class QueueJoinTimeout(RemouladeError):
    """Raised by brokers that support joining on queues when the join
    operation times out.
    """


class ConnectionError(BrokerError):
    """Base class for broker connection-related errors."""


class ConnectionFailed(ConnectionError):
    """Raised when a broker connection could not be opened."""


class ConnectionClosed(ConnectionError):
    """Raised when a broker connection is suddenly closed."""


class RateLimitExceeded(RemouladeError):
    """Raised when a rate limit has been exceeded."""


class NoResultBackend(BrokerError):
    """Raised when trying to access the result backend on a broker without it"""


class NoCancelBackend(BrokerError):
    """Raised when trying to access the cancel backend on a broker without it"""


class NoStateBackend(BrokerError):
    """Raised when trying to access the state backend on a broker without it"""


class ChannelPoolTimeout(BrokerError):
    """Raised when the broker has wait for tool long to fetch a channel from its channel pool."""


class InvalidProgress(RemouladeError):
    """Raised when trying to set a progress that is greater than 1 or less than 0"""


class NoScheduler(RemouladeError):
    """Raised when trying to get a scheduler when there is not it"""
