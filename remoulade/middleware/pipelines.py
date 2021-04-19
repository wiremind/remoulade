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
from ..errors import NoResultBackend
from ..logging import get_logger
from .middleware import Middleware


class Pipelines(Middleware):
    """Middleware that lets you pipe actors together so that the
    output of one actor feeds into the input of another.

    Parameters:
      pipe_ignore(bool): When True, ignores the result of the previous
        actor in the pipeline.
      pipe_target(dict): A message representing the actor the current
        result should be fed into.
    """

    def __init__(self):
        self.logger = get_logger(__name__, type(self))

    @property
    def actor_options(self):
        return {
            "pipe_ignore",
            "pipe_target",
        }

    def after_process_message(self, broker, message, *, result=None, exception=None):
        from ..composition import GroupInfo

        if exception is not None or getattr(message, "failed", False):
            return

        pipe_target = message.options.get("pipe_target")
        group_info = message.options.get("group_info")
        if group_info:
            group_info = GroupInfo(**group_info)

        if pipe_target is not None:

            try:
                if group_info and not self._group_completed(message, group_info, broker):
                    return

                self._send_next_message(pipe_target, broker, result, group_info)

                broker.emit_after("enqueue_pipe_target", group_info)

            except NoResultBackend as e:
                self.logger.error(str(e))
                message.fail()

    def _send_next_message(self, pipe_target, broker, result, group_info):
        """ Send a message to the pipe target (if it's a list to all the pipe targets)

        If it's a group, we need to get the result of each actor from the result backend because it's not present in
        the message.

        Parameters:
            pipe_target(dict|List[dict]): the message or list of message to enqueue
            broker(Broker): the broker
            result(any): the result of the actor (to be send with the message if pipe_ignore=False)
            group_info(GroupInfo|None): the info of the group if it's a group else None
        """
        # Since Pipelines is a default middleware, this import has to
        # happen at runtime in order to avoid a cyclic dependency
        # from broker -> pipelines -> messages -> broker.
        from ..message import Message

        for message_data in pipe_target:
            next_message = Message(**message_data)
            next_actor = broker.get_actor(next_message.actor_name)
            pipe_ignore = next_message.options.get("pipe_ignore") or next_actor.options.get("pipe_ignore")

            if not pipe_ignore:
                if group_info:
                    result = self._group_results(group_info, broker)
                next_message = next_message.copy(args=next_message.args + (result,))

            broker.enqueue(next_message)

    @staticmethod
    def _group_results(group_info, broker):
        """ Get the result of a group (fetch the group members message_ids then all the results) """
        from ..collection_results import CollectionResults

        try:
            result_backend = broker.get_result_backend()
        except NoResultBackend:
            raise NoResultBackend("Pipeline with groups are ony supported with a result backend")

        message_ids = result_backend.get_group_message_ids(group_id=group_info.group_id)
        results = CollectionResults.from_message_ids(message_ids)
        return list(results.get())

    def _group_completed(self, message, group_info, broker):
        """ Returns true if a group is completed, and increment the completion count of the group

        Parameters:
            group_info(GroupInfo): the info of the group to get the completion from
            broker(Broker): the broker to use

        Raises:
            NoResultBackend: if there is no result backend set
        """
        try:
            result_backend = broker.get_result_backend()
        except NoResultBackend:
            raise NoResultBackend("Pipeline with groups are ony supported with a result backend")

        with result_backend.retry(broker, message, self.logger):
            group_completion = result_backend.increment_group_completion(group_info.group_id, message.message_id)
        group_competed = group_completion >= group_info.children_count
        if group_competed:
            self.logger.info("Finished group %s.", group_info.group_id)
        if group_completion == 1:
            self.logger.info("First message of Group %s has been completed.", group_info.group_id)
        return group_competed
