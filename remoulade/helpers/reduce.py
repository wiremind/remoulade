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
from ..composition import group


def reduce(messages, merge_actor, cancel_on_error=False, size=2, merge_kwargs=None):
    """Recursively merge messages

    Parameters:
      messages(Iterator[Message|pipeline]): A sequence of messages or pipelines that needs to be merged.
      merge_actor(Actor): The actor that will be responsible for the merge of two messages.
      cancel_on_error(boolean): True if you want to cancel all messages of a group if one of
        the actor fails, this is only possible with a Cancel middleware.
      size(int): Number of messages that are reduced at once.
      merge_kwargs: kwargs to be passed to each merge message.

    Returns:
      Message|pipeline: a message or a pipeline that will return the reduced result of all the given messages.

    Raise:
        NoCancelBackend: if no cancel middleware is set
    """
    if merge_kwargs is None:
        merge_kwargs = {}
    messages = list(messages)
    while len(messages) > 1:
        reduced_messages = []
        for i in range(0, len(messages), size):
            if i == len(messages) - (size - 1):
                reduced_messages.append(messages[i])
            else:
                grouped_message = group(messages[i : i + size], cancel_on_error=cancel_on_error)
                reduced_messages.append(grouped_message | merge_actor.message(**merge_kwargs))
        messages = reduced_messages

    return messages[0]
