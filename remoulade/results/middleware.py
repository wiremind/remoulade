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
from ..logging import get_logger
from ..middleware import Middleware, Retries
from .backend import BackendResult
from .errors import ParentFailed

#: The maximum amount of milliseconds results are allowed to exist in
#: the backend.
DEFAULT_RESULT_TTL = 600000


class Results(Middleware):
    """Middleware that automatically stores actor results.

    Example:

      >>> from remoulade.results import Results
      >>> from remoulade.results.backends import RedisBackend
      >>> backend = RedisBackend()
      >>> broker.add_middleware(Results(backend=backend))

      >>> @remoulade.actor(store_results=True)
      ... def add(x, y):
      ...   return x + y

      >>> broker.declare_actor(add)
      >>> message = add.send(1, 2)
      >>> message.result.get()
      3

    Parameters:
      backend(ResultBackend): The result backend to use when storing
        results.
      store_results(bool): Whether or not actor results should be
        stored.  Defaults to False and can be set on a per-actor
        basis.
      result_ttl(int): The maximum number of milliseconds results are
        allowed to exist in the backend.  Defaults to 10 minutes and
        can be set on a per-actor basis.
    """

    # Warning: Results need to be before Retries to work
    default_before = Retries

    def __init__(self, *, backend=None, store_results=False, result_ttl=None):
        self.logger = get_logger(__name__, type(self))
        self.backend = backend
        self.store_results = store_results
        self.result_ttl = result_ttl or DEFAULT_RESULT_TTL

    @property
    def actor_options(self):
        return {"store_results", "result_ttl"}

    def after_process_message(self, broker, message, *, result=None, exception=None):
        actor = broker.get_actor(message.actor_name)

        store_results = message.options.get("store_results", actor.options.get("store_results", self.store_results))
        result_ttl = actor.options.get("result_ttl", self.result_ttl)
        message_failed = getattr(message, "failed", False)

        results = []
        if store_results:
            if exception is None:
                results.append((message.message_id, BackendResult(result=result, error=None)))
            elif message_failed:
                error_str = self._serialize_exception(exception)
                results.append((message.message_id, BackendResult(result=None, error=error_str)))

        # even if the actor do not have store_results, we need to invalidate the messages in the pipeline that has it
        if message_failed:
            error_str = self._serialize_exception(exception)
            exception = ParentFailed("%s failed because of %s" % (message, error_str))
            children_result = BackendResult(result=None, error=self._serialize_exception(exception))

            for message_id in self._get_children_message_ids(broker, message.options.get("pipe_target")):
                results.append((message_id, children_result))

        if results:
            message_ids, results = zip(*results)
            with self.backend.retry(broker, message, self.logger):
                self.backend.store_results(message_ids, results, result_ttl)

    @staticmethod
    def _serialize_exception(exception):
        try:
            return repr(exception)
        except Exception as e:  # noqa
            return "Exception could not be serialized"

    def _get_children_message_ids(self, broker, pipe_target):
        """ Get the ids of all the following messages in the pipeline which have store_results """
        from ..message import Message

        message_ids = set()

        if isinstance(pipe_target, list):
            for message_data in pipe_target:
                message_ids |= self._get_children_message_ids(broker, message_data)
        elif pipe_target:
            message = Message(**pipe_target)
            actor = broker.get_actor(message.actor_name)

            if message.options.get("store_results", actor.options.get("store_results", self.store_results)):
                message_ids.add(message.message_id)

            message_ids |= self._get_children_message_ids(broker, message.options.get("pipe_target"))

        return message_ids

    def after_enqueue_pipe_target(self, _, group_info):
        """ After a pipe target has been enqueued, we need to forget the result of the group (if it's a group) """
        if group_info:
            message_ids = self.backend.get_group_message_ids(group_info.group_id)
            self.backend.forget_results(message_ids, self.result_ttl)
            self.backend.delete_group_message_ids(group_info.group_id)

    def before_build_group_pipeline(self, _, group_id, message_ids):
        self.backend.set_group_message_ids(group_id, message_ids, self.result_ttl)
