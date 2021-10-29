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
import time
from itertools import chain
from queue import Empty, Queue
from typing import List, Optional

from ..broker import Broker, Consumer, MessageProxy
from ..common import current_millis
from ..errors import QueueNotFound
from ..helpers.queues import dq_name, iter_queue, join_queue
from ..message import Message


class StubBroker(Broker):
    """A broker that can be used within unit tests.

    Attributes:
      dead_letters(list[Message]): Contains the dead-lettered messages
        for all defined queues.
    """

    def __init__(self, middleware=None):
        super().__init__(middleware)

        self.dead_letters: List[Message] = []

    def consume(self, queue_name, prefetch=1, timeout=100):
        """Create a new consumer for a queue.

        Parameters:
          queue_name(str): The queue to consume.
          prefetch(int): The number of messages to prefetch.
          timeout(int): The idle timeout in milliseconds.

        Raises:
          QueueNotFound: If the queue hasn't been declared.

        Returns:
          Consumer: A consumer that retrieves messages from Redis.
        """
        try:
            return _StubConsumer(self.queues[queue_name], self.dead_letters, timeout)
        except KeyError as e:
            raise QueueNotFound(queue_name) from e

    def declare_queue(self, queue_name):
        """Declare a queue.  Has no effect if a queue with the given
        name has already been declared.

        Parameters:
          queue_name(str): The name of the new queue.
        """
        if queue_name not in self.queues:
            self.emit_before("declare_queue", queue_name)
            self.queues[queue_name] = Queue()
            self.emit_after("declare_queue", queue_name)

            delayed_name = dq_name(queue_name)
            self.queues[delayed_name] = Queue()
            self.delay_queues.add(delayed_name)
            self.emit_after("declare_delay_queue", delayed_name)

    def _apply_delay(self, message: "Message", delay: Optional[int] = None) -> "Message":
        if delay is not None:
            message_eta = current_millis() + delay
            queue_name = message.queue_name if delay is None else dq_name(message.queue_name)
            message = message.copy(queue_name=queue_name, options={"eta": message_eta})

        return message

    def _enqueue(self, message, *, delay=None):
        """Enqueue a message.

        Parameters:
          message(Message): The message to enqueue.
          delay(int): The minimum amount of time, in milliseconds, to
            delay the message by.

        Raises:
          QueueNotFound: If the queue the message is being enqueued on
            doesn't exist.
        """
        queue_name = message.queue_name

        if queue_name not in self.queues:
            raise QueueNotFound(queue_name)

        self.queues[queue_name].put(message.encode())
        return message

    def flush(self, queue_name):
        """Drop all the messages from a queue.

        Parameters:
          queue_name(str): The queue to flush.
        """
        for _ in iter_queue(self.queues[queue_name]):
            self.queues[queue_name].task_done()

    def flush_all(self):
        """Drop all messages from all declared queues."""
        for queue_name in chain(self.queues, self.delay_queues):
            self.flush(queue_name)

    def join(self, queue_name, *, timeout=None):
        """Wait for all the messages on the given queue to be
        processed.  This method is only meant to be used in tests
        to wait for all the messages in a queue to be processed.

        Raises:
          QueueJoinTimeout: When the timeout elapses.
          QueueNotFound: If the given queue was never declared.

        Parameters:
          queue_name(str): The queue to wait on.
          timeout(Optional[int]): The max amount of time, in
            milliseconds, to wait on this queue.
        """
        try:
            deadline = timeout and time.monotonic() + timeout / 1000
            while True:
                for name in [queue_name, dq_name(queue_name)]:
                    timeout = deadline and deadline - time.monotonic()
                    join_queue(self.queues[name], timeout=timeout)

                # We cycle through $queue then $queue.DQ then $queue
                # again in case the messages that were on the DQ got
                # moved back on $queue.
                for name in [queue_name, dq_name(queue_name)]:
                    if self.queues[name].unfinished_tasks:
                        break
                else:
                    return
        except KeyError as e:
            raise QueueNotFound(queue_name) from e


class _StubConsumer(Consumer):
    def __init__(self, queue, dead_letters, timeout):
        self.queue = queue
        self.dead_letters = dead_letters
        self.timeout = timeout

    def ack(self, message):
        self.queue.task_done()

    def nack(self, message):
        self.queue.task_done()
        self.dead_letters.append(message)

    def __next__(self):
        try:
            data = self.queue.get(timeout=self.timeout / 1000)
            message = Message.decode(data)
            return MessageProxy(message)
        except Empty:
            return None
