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

from .actor import Actor, actor
from .broker import Broker, Consumer, MessageProxy, change_broker, declare_actors, get_broker, set_broker
from .collection_results import CollectionResults
from .composition import group, pipeline
from .encoder import Encoder, JSONEncoder, PickleEncoder
from .errors import (
    ActorNotFound,
    BrokerError,
    ChannelPoolTimeout,
    ConnectionClosed,
    ConnectionError,
    ConnectionFailed,
    NoResultBackend,
    QueueJoinTimeout,
    QueueNotFound,
    RateLimitExceeded,
    RemouladeError,
)
from .generic import GenericActor
from .logging import get_logger
from .message import Message, get_encoder, set_encoder
from .middleware import Middleware
from .result import Result
from .utils import get_scheduler, set_scheduler
from .worker import Worker

__all__ = [
    # Actors
    "Actor",
    "GenericActor",
    "actor",
    # Brokers
    "Broker",
    "Consumer",
    "MessageProxy",
    "get_broker",
    "set_broker",
    "change_broker",
    "declare_actors",
    # Composition
    "group",
    "pipeline",
    "CollectionResults",
    # Encoding
    "Encoder",
    "JSONEncoder",
    "PickleEncoder",
    # Errors
    "RemouladeError",
    "BrokerError",
    "ActorNotFound",
    "QueueNotFound",
    "QueueJoinTimeout",
    "ConnectionError",
    "ConnectionClosed",
    "ConnectionFailed",
    "RateLimitExceeded",
    "NoResultBackend",
    "ChannelPoolTimeout",
    # Logging
    "get_logger",
    # Messages
    "Message",
    "get_encoder",
    "set_encoder",
    "Result",
    # Middlware
    "Middleware",
    # Workers
    "Worker",
    # Scheduler
    "get_scheduler",
    "set_scheduler",
]

__version__ = "0.30.0"
