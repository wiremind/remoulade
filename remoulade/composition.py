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
from collections import namedtuple
from contextlib import nullcontext
from typing import TYPE_CHECKING, Iterable, List, Optional, Union, cast

from typing_extensions import TypedDict

from .broker import get_broker
from .collection_results import CollectionResults
from .common import flatten, generate_unique_id

if TYPE_CHECKING:
    from .message import Message  # noqa
    from .result import Result


class GroupInfoDict(TypedDict):
    group_id: str
    children_count: int


class GroupInfo(namedtuple("GroupInfo", ("group_id", "children_count"))):
    """Encapsulates metadata about a group being sent to multiple actors.

    Parameters:
      group_id(str): The id of the group
      children_count(int)
    """

    def __new__(cls, *, group_id: str, children_count: int):
        return super().__new__(cls, group_id, children_count)

    def asdict(self) -> GroupInfoDict:
        return cast(GroupInfoDict, self._asdict())


class pipeline:
    """Chain actors together, passing the result of one actor to the
    next one in line.

    Parameters:
      children(Iterable[Message|pipeline|group]): A sequence of messages or
        pipelines or groups.  Child pipelines are flattened into the resulting
        pipeline.
      broker(Broker): The broker to run the pipeline on.  Defaults to
        the current global broker.
      cancel_on_error(boolean): True if you want to cancel all messages of a composition if on of
      the actor fails, this is only possible with a Cancel middleware.

    Attributes:
        children(List[Message|group]) The sequence of messages or groups to execute as a pipeline
    """

    def __init__(self, children: "Iterable[Union[Message, pipeline, group]]", cancel_on_error: bool = False):
        self.broker = get_broker()

        self.children: List[Union[Message, group]] = []
        for child in children:
            if isinstance(child, pipeline):
                self.children += child.children
            elif isinstance(child, group):
                self.children.append(child)
            else:
                self.children.append(child.copy())

        self.cancel_on_error = cancel_on_error
        if cancel_on_error:
            self.broker.get_cancel_backend()

    def build(self, *, last_options=None, composition_id: str = None, cancel_on_error: bool = False):
        """Build the pipeline, return the first message to be enqueued or integrated in another pipeline

        Build the pipeline by starting at the end. We build a message with all it's options in one step and
        we serialize it (asdict) as the previous message pipe_target in the next step.

        We need to know what is the options (pipe_target) of the pipeline before building it because we cannot
        edit the pipeline after it has been built.

        Parameters:
            cancel_on_error(bool): Whether the whole composition should be canceled when one of its messages fails
            composition_id(str): The composition id to pass to messages
            last_options(dict): options to be assigned to the last actor of the pipeline (ex: pipe_target)

        Returns:
            the first message of the pipeline
        """
        composition_id = composition_id or generate_unique_id()
        cancel_on_error = cancel_on_error or self.cancel_on_error
        next_child = None
        for child in reversed(self.children):
            if next_child:
                options = {"pipe_target": [m.asdict() for m in next_child]}
            else:
                options = last_options or {}

            options["composition_id"] = composition_id
            options["cancel_on_error"] = cancel_on_error

            if isinstance(child, group):
                next_child = child.build(options)
            else:
                next_child = [child.build(options)]

        return next_child

    def __len__(self):
        """Returns the length of the pipeline."""
        return len(self.children)

    def __or__(self, other: "Union[Message, group]"):
        """Returns a new pipeline with "other" added to the end."""
        return type(self)(self.children + [other])

    def __str__(self):  # pragma: no cover
        return "pipeline([%s])" % ", ".join(str(m) for m in self.children)

    @property
    def message_ids(self):
        for child in self.children:
            if isinstance(child, group):
                yield list(child.message_ids)
            else:
                yield child.message_id

    def run(self, *, delay: Optional[int] = None, transaction: Optional[bool] = None) -> "pipeline":
        """Run this pipeline.

        Parameters:
          delay(int): The minimum amount of time, in milliseconds, the
            pipeline should be delayed by.

        Returns:
          pipeline: Itself.
        """
        transaction = transaction if transaction is not None else self.broker.group_transaction
        with self.broker.tx() if transaction else nullcontext():
            first = self.build()
            if isinstance(first, list):
                for message in first:
                    self.broker.enqueue(message, delay=delay)
            else:
                self.broker.enqueue(first, delay=delay)
        return self

    @property
    def results(self) -> CollectionResults:
        """CollectionResults created from this pipeline, used for result related methods"""
        results: List[Union[Result, CollectionResults]] = []
        for element in self.children:
            results += [element.results if isinstance(element, group) else element.result]
        return CollectionResults(results)

    @property
    def result(self):
        """Result of the last message/group of the pipeline"""
        last_child = self.children[-1]
        return last_child.results if isinstance(last_child, group) else last_child.result

    def cancel(self) -> None:
        """Mark all the children as cancelled"""
        broker = get_broker()
        backend = broker.get_cancel_backend()
        backend.cancel(list(flatten(self.message_ids)))


class group:
    """Run a group of actors in parallel.

    Parameters:
      children(Iterable[Message|pipeline]): A sequence of messages or pipelines.

    Attributes:
        children(List[Message|pipeline]) The sequence to execute as a group

    Raise:
        NoCancelBackend: if no cancel middleware is set
    """

    def __init__(
        self,
        children: "Iterable[Union[Message, pipeline]]",
        *,
        group_id: Optional[str] = None,
        cancel_on_error: bool = False,
    ) -> None:
        self.children: "List[Union[Message, pipeline]]" = []
        for child in children:
            if isinstance(child, group):
                raise ValueError("Groups of groups are not supported")
            self.children.append(child)

        self.broker = get_broker()
        self.group_id: str = generate_unique_id() if group_id is None else group_id

        self.cancel_on_error = cancel_on_error
        if cancel_on_error:
            self.broker.get_cancel_backend()

    def __or__(self, other: "Union[Message, group, pipeline]") -> pipeline:
        """Combine this group into a pipeline with "other"."""
        return pipeline([self, other])

    def __len__(self) -> int:
        """Returns the size of the group."""
        return len(self.children)

    def __str__(self) -> str:  # pragma: no cover
        return f"group({', '.join(str(child) for child in self.children)})"

    def build(self, options=None) -> "List[Message]":
        """Build group for pipeline"""
        if options is None:
            options = {}
        else:
            self.broker.emit_before("build_group_pipeline", group_id=self.group_id, message_ids=list(self.message_ids))

        composition_id = options.get("composition_id", self.group_id)
        cancel_on_error = options.get("composition_id", self.cancel_on_error)
        options = {
            "group_info": self.info.asdict(),
            "composition_id": composition_id,
            "cancel_on_error": self.cancel_on_error,
            **options,
        }
        messages: "List[Message]" = []
        for group_child in self.children:
            if isinstance(group_child, pipeline):
                messages += group_child.build(
                    last_options=options, composition_id=composition_id, cancel_on_error=cancel_on_error
                )
            else:
                messages += [group_child.build(options)]
        return messages

    @property
    def info(self) -> GroupInfo:
        """Info used for group completion and cancel"""
        return GroupInfo(group_id=self.group_id, children_count=len(self.children))

    @property
    def message_ids(self):
        for child in self.children:
            if isinstance(child, pipeline):
                yield list(child.message_ids)
            else:
                yield child.message_id

    def run(self, *, delay: Optional[int] = None, transaction: Optional[bool] = None) -> "group":
        """Run the actors in this group.

        Parameters:
          delay(int): The minimum amount of time, in milliseconds,
            each message in the group should be delayed by.
          transaction(bool):
        """
        transaction = transaction if transaction is not None else self.broker.group_transaction
        with self.broker.tx() if transaction else nullcontext():
            for message in self.build():
                self.broker.enqueue(message, delay=delay)

        return self

    @property
    def results(self) -> CollectionResults:
        """CollectionResults created from this group, used for result related methods"""
        return CollectionResults(children=[child.result for child in self.children])

    def cancel(self) -> None:
        """Mark all the children as cancelled"""
        broker = get_broker()
        backend = broker.get_cancel_backend()
        backend.cancel(list(flatten(self.message_ids)))
