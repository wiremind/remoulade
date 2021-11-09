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

import traceback
from typing import Callable, Optional

from ..helpers import compute_backoff
from ..helpers.backoff import BackoffStrategy
from ..logging import get_logger
from .middleware import Middleware

#: The default minimum amount of backoff to apply to retried tasks.
DEFAULT_MIN_BACKOFF = 15000

#: The default maximum amount of backoff to apply to retried tasks.
DEFAULT_MAX_BACKOFF = 1000 * 60 * 60


class Retries(Middleware):
    """Middleware that automatically retries failed tasks with
    exponential backoff.

    Parameters:
      max_retries(int): The maximum number of times tasks can be retried.
      min_backoff(int): The minimum amount of backoff milliseconds to
        apply to retried tasks.  Defaults to 15 seconds.
      max_backoff(int): The maximum amount of backoff milliseconds to
        apply to retried tasks.  Defaults to 7 days.
      retry_when(Callable[[int, Exception], bool]): An optional
        predicate that can be used to programmatically determine
        whether a task should be retried or not.  This takes
        precedence over `max_retries` when set.
    """

    def __init__(
        self,
        *,
        max_retries: Optional[int] = None,
        min_backoff: Optional[int] = None,
        max_backoff: Optional[int] = None,
        retry_when: Optional[Callable[[int, Exception], bool]] = None,
        backoff_strategy: BackoffStrategy = "exponential",
        jitter: bool = True,
    ):
        self.logger = get_logger(__name__, type(self))
        self.max_retries = max_retries
        self.min_backoff = min_backoff or DEFAULT_MIN_BACKOFF
        self.max_backoff = max_backoff or DEFAULT_MAX_BACKOFF
        self.retry_when = retry_when
        self.backoff_strategy = backoff_strategy
        self.jitter = jitter

    @property
    def actor_options(self):
        return {"max_retries", "min_backoff", "max_backoff", "retry_when", "backoff_strategy", "jitter"}

    def after_process_message(self, broker, message, *, result=None, exception=None):
        if exception is None:
            return

        retries = message.options.setdefault("retries", 0)
        max_retries = self.get_option("max_retries", broker=broker, message=message)
        retry_when = self.get_option("retry_when", broker=broker, message=message)
        if retry_when is None and not max_retries:
            message.fail()
            return

        if (
            retry_when is not None
            and not retry_when(retries, exception)
            or max_retries is not None
            and retries >= max_retries
        ):
            if max_retries is not None and retries >= max_retries:
                self.logger.warning(f"Retries exceeded for message {message.message_id}.")
            else:
                self.logger.warning(f"Message {message.message_id} has failed and will not be retried.")
            message.fail()
            return

        new_message = message.copy()
        new_message.options["retries"] += 1
        new_message.options["traceback"] = traceback.format_exc(limit=30)
        min_backoff = self.get_option("min_backoff", broker=broker, message=message)
        max_backoff = self.get_option("max_backoff", broker=broker, message=message)
        backoff_strategy = self.get_option("backoff_strategy", broker=broker, message=message)
        jitter = self.get_option("jitter", broker=broker, message=message)
        _, backoff = compute_backoff(
            retries,
            min_backoff=min_backoff,
            max_backoff=max_backoff,
            jitter=jitter,
            max_retries=max_retries or 10,
            backoff_strategy=backoff_strategy,
        )
        self.logger.info("Retrying message %r in %d milliseconds.", message.message_id, backoff)
        broker.enqueue(new_message, delay=backoff)
