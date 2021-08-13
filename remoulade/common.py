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
import uuid
from collections import Iterable
from itertools import islice
from queue import Empty
from time import time
from typing import Iterator, List

from .errors import QueueJoinTimeout


def current_millis():
    """Returns the current UNIX time in milliseconds."""
    return int(time() * 1000)


def iter_queue(queue):
    """Iterate over the messages on a queue until it's empty.  Does
    not block while waiting for messages.

    Parameters:
      queue(Queue): The queue to iterate over.

    Returns:
      generator[object]: The iterator.
    """
    while True:
        try:
            yield queue.get_nowait()
        except Empty:
            break


def join_queue(queue, timeout=None):
    """The join() method of standard queues in Python doesn't support
    timeouts.  This implements the same functionality as that method,
    with optional timeout support, by depending the internals of
    Queue.

    Raises:
      QueueJoinTimeout: When the timeout is reached.

    Parameters:
      timeout(Optional[float])
    """
    with queue.all_tasks_done:
        while queue.unfinished_tasks:
            finished_in_time = queue.all_tasks_done.wait(timeout=timeout)
            if not finished_in_time:
                raise QueueJoinTimeout("timed out after %.02f seconds" % timeout)


def join_all(joinables, timeout):
    """Wait on a list of objects that can be joined with a total
    timeout represented by ``timeout``.

    Parameters:
      joinables(object): Objects with a join method.
      timeout(int): The total timeout in milliseconds.
    """
    started, elapsed = current_millis(), 0
    for ob in joinables:
        ob.join(timeout=timeout / 1000)
        elapsed = current_millis() - started
        timeout = max(0, timeout - elapsed)


def q_name(queue_name):
    """Returns the canonical queue name for a given queue."""
    if queue_name.endswith(".DQ") or queue_name.endswith(".XQ"):
        return queue_name[:-3]
    return queue_name


def dq_name(queue_name):
    """Returns the delayed queue name for a given queue.  If the given
    queue name already belongs to a delayed queue, then it is returned
    unchanged.
    """
    if queue_name.endswith(".DQ"):
        return queue_name

    if queue_name.endswith(".XQ"):
        queue_name = queue_name[:-3]
    return queue_name + ".DQ"


def xq_name(queue_name):
    """Returns the dead letter queue name for a given queue.  If the
    given queue name belongs to a delayed queue, the dead letter queue
    name for the original queue is generated.
    """
    if queue_name.endswith(".XQ"):
        return queue_name

    if queue_name.endswith(".DQ"):
        queue_name = queue_name[:-3]
    return queue_name + ".XQ"


def generate_unique_id() -> str:
    """Generate a globally-unique message id."""
    return str(uuid.uuid4())


def flatten(iterable):
    """Flatten deep an iterable"""
    for el in iterable:
        if isinstance(el, Iterable) and not isinstance(el, (str, bytes)):
            yield from flatten(el)
        else:
            yield el


def chunk(iterable: Iterable, size: int) -> Iterator[List]:
    """Returns an iterator of a list of length size"""
    i = iter(iterable)
    piece = list(islice(i, size))
    while piece:
        yield piece
        piece = list(islice(i, size))
