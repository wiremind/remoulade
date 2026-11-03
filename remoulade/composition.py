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
from __future__ import annotations

from collections import namedtuple
from collections.abc import Iterable
from contextlib import nullcontext
from typing import TYPE_CHECKING, Any, Self, TypedDict, TypeVar, cast, overload

from .broker import get_broker
from .collection_results import CollectionResults
from .common import flatten, generate_unique_id

if TYPE_CHECKING:
    from .message import Message
    from .result import Result

ResultsT = TypeVar("ResultsT", bound="Result[Any] | CollectionResults[Any]")
ResultsT_1 = TypeVar("ResultsT_1", bound="Result[Any] | CollectionResults[Any]")


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


class _BasePipeline[ResultsT: "Result[Any] | CollectionResults[Any]"]:
    """Implementation shared by :class:`pipeline` and :class:`unsafe_pipeline`, which only differ by the groups
    they accept."""

    def __init__(
        self,
        children: tuple[Message[Any] | _BasePipeline[Any] | group[Any], ...],
        cancel_on_error: bool = False,
    ):
        self.broker = get_broker()

        self.children: list[Message[Any] | group[Any]] = []
        for child in children:
            if isinstance(child, _BasePipeline):
                self.children += child.children
            elif isinstance(child, group):
                self.children.append(child)
            else:
                self.children.append(child.copy())

        self.cancel_on_error = cancel_on_error
        if cancel_on_error:
            self.broker.get_cancel_backend()

    def build(self, *, last_options=None, composition_id: str | None = None, cancel_on_error: bool = False):
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
            options: dict[str, bool | str | list[Any]] = (
                {"pipe_target": [m.asdict() for m in next_child]} if next_child else last_options or {}
            )

            options["composition_id"] = composition_id
            options["cancel_on_error"] = cancel_on_error

            built_children = child.build(options) if isinstance(child, group) else [child.build(options)]
            if not built_children:
                # Skip empty groups so downstream children still run
                continue

            next_child = built_children

        return next_child

    def __len__(self):
        """Returns the length of the pipeline."""
        return len(self.children)

    def __or__(self, other: Message | group):
        """Returns a new pipeline with "other" added to the end."""
        return type(self)((*tuple(self.children), other))

    def __str__(self):  # pragma: no cover
        return "pipeline([{}])".format(", ".join(str(m) for m in self.children))

    @property
    def message_ids(self):
        for child in self.children:
            if isinstance(child, group):
                yield list(child.message_ids)
            else:
                yield child.message_id

    def run(self, *, delay: int | None = None, transaction: bool | None = None) -> Self:
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
    def results(self) -> CollectionResults[Any]:
        """CollectionResults created from this pipeline, used for result related methods"""
        results: list[Result | CollectionResults] = []
        for element in self.children:
            results += [element.results if isinstance(element, group) else element.result]
        return CollectionResults(results)

    @property
    def result(self) -> ResultsT:
        """Result of the last message/group of the pipeline"""
        last_child = self.children[-1]
        return cast(ResultsT, last_child.results if isinstance(last_child, group) else last_child.result)

    def cancel(self) -> None:
        """Mark all the children as cancelled"""
        broker = get_broker()
        backend = broker.get_cancel_backend()
        backend.cancel(list(flatten(self.message_ids)))


class pipeline[ResultsT: "Result[Any] | CollectionResults[Any]"](_BasePipeline[ResultsT]):
    """Chain actors together, passing the result of one actor to the
    next one in line.

    A pipeline holds at most one group. Each message of a pipeline carries
    every message that follows it, so with two groups each message of the
    first group would carry every message of the second one: the payload
    would grow as the product of the group sizes. See :class:`unsafe_pipeline`.

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

    Raises:
      ValueError: if the pipeline holds more than one group once its child pipelines are flattened.
    """

    # A variadic tuple can only have one unbounded part, so the overloads below cannot express "one group anywhere":
    # they type a group in first, second, second to last or last position.
    # We should actually not use ResultTs in Message[ResultsT] but define a new type var that is only bound to Result
    # but then mypy gets lost, so reusing ResultsT and ignoring the error.

    @overload
    def __init__(
        self,
        children: tuple[  # type: ignore
            *tuple[Message[Any] | pipeline[Any], ...], Message[ResultsT] | pipeline[ResultsT]
        ],
        cancel_on_error: bool = False,
    ): ...

    @overload
    def __init__(
        self: pipeline[CollectionResults[ResultsT_1]],
        children: tuple[*tuple[Message[Any] | pipeline[Any], ...], group[ResultsT_1]],
        cancel_on_error: bool = False,
    ): ...

    @overload
    def __init__(
        self,
        children: tuple[  # type: ignore
            group[Any], *tuple[Message[Any] | pipeline[Any], ...], Message[ResultsT] | pipeline[ResultsT]
        ],
        cancel_on_error: bool = False,
    ): ...

    @overload
    def __init__(
        self,
        children: tuple[  # type: ignore
            Message[Any] | pipeline[Any],
            group[Any],
            *tuple[Message[Any] | pipeline[Any], ...],
            Message[ResultsT] | pipeline[ResultsT],
        ],
        cancel_on_error: bool = False,
    ): ...

    @overload
    def __init__(
        self,
        children: tuple[  # type: ignore
            *tuple[Message[Any] | pipeline[Any], ...], group[Any], Message[ResultsT] | pipeline[ResultsT]
        ],
        cancel_on_error: bool = False,
    ): ...

    @overload
    def __init__(
        self,
        children: tuple[Message[Any] | pipeline[Any], ...],
        cancel_on_error: bool = False,
    ): ...

    def __init__(
        self,
        children: tuple[Message[Any] | _BasePipeline[Any] | group[Any], ...],
        cancel_on_error: bool = False,
    ):
        super().__init__(children, cancel_on_error)
        if sum(isinstance(child, group) for child in self.children) > 1:
            raise ValueError(
                "A pipeline holds at most one group: each message of a group would carry every message of the next "
                "group, so the payload would grow as the product of the group sizes. "
                "Use unsafe_pipeline to compose several groups anyway."
            )


class unsafe_pipeline[ResultsT: "Result[Any] | CollectionResults[Any]"](_BasePipeline[ResultsT]):
    """Like :class:`pipeline`, but may hold several groups.

    .. warning::
       Use it with caution. Each message of a pipeline carries every message
       that follows it, so each message of a group carries every message of
       the next group: the payload grows as the product of the group sizes,
       and a few groups of a few hundred messages are enough to enqueue
       messages of hundreds of megabytes. Keep the groups small, and make sure
       their sizes stay bounded whatever the input data.

    Use a :class:`pipeline` whenever possible, and this class when a
    composition with several groups is still needed.

    An unsafe_pipeline is not a :class:`pipeline`: a pipeline does not accept
    it as a child, so ``message | unsafe`` raises if ``unsafe`` holds a group
    and ``unsafe_pipeline((message, unsafe))`` must be used instead.

    Parameters:
      children(Iterable[Message|pipeline|unsafe_pipeline|group]): A sequence of messages,
        pipelines or groups.  Child pipelines are flattened into the resulting
        pipeline.
      cancel_on_error(boolean): True if you want to cancel all messages of a composition if on of
      the actor fails, this is only possible with a Cancel middleware.

    Attributes:
        children(List[Message|group]) The sequence of messages or groups to execute as a pipeline
    """

    @overload
    def __init__(
        self,
        # we should actually not use ResultTs here but define a new type var that is only bound to Result
        # but then mypy gets lost, so reusing ResultsT and ignoring the error
        children: tuple[  # type: ignore
            *tuple[Message[Any] | pipeline[Any] | unsafe_pipeline[Any] | group[Any], ...],
            Message[ResultsT] | pipeline[ResultsT] | unsafe_pipeline[ResultsT],
        ],
        cancel_on_error: bool = False,
    ): ...

    @overload
    def __init__(
        self: unsafe_pipeline[CollectionResults[ResultsT_1]],
        children: tuple[
            *tuple[Message[Any] | pipeline[Any] | unsafe_pipeline[Any] | group[Any], ...], group[ResultsT_1]
        ],
        cancel_on_error: bool = False,
    ): ...

    @overload
    def __init__(
        self,
        children: tuple[Message[Any] | pipeline[Any] | unsafe_pipeline[Any] | group[Any], ...],
        cancel_on_error: bool = False,
    ): ...

    def __init__(
        self,
        children: tuple[Message[Any] | _BasePipeline[Any] | group[Any], ...],
        cancel_on_error: bool = False,
    ):
        super().__init__(children, cancel_on_error)


class group[ResultsT: "Result[Any] | CollectionResults[Any]"]:
    """Run a group of actors in parallel.

    Parameters:
      children(Iterable[Message|pipeline|unsafe_pipeline]): A sequence of messages or pipelines.

    Attributes:
        children(List[Message|pipeline|unsafe_pipeline]) The sequence to execute as a group

    Raise:
        NoCancelBackend: if no cancel middleware is set
    """

    def __init__(
        self,
        # we should actually not use ResultTs here but define a new type var that is only bound to Result
        # but then mypy gets lost, so reusing ResultsT and ignoring the error
        children: Iterable[pipeline[ResultsT] | unsafe_pipeline[ResultsT] | Message[ResultsT]],  # type: ignore
        group_id: str | None = None,
        cancel_on_error: bool = False,
    ) -> None:
        self.children: list[Message[Any] | _BasePipeline[Any]] = []
        for child in children:
            if isinstance(child, group):
                raise ValueError("Groups of groups are not supported")
            self.children.append(child)

        self.broker = get_broker()
        self.group_id: str = generate_unique_id() if group_id is None else group_id

        self.cancel_on_error = cancel_on_error
        if cancel_on_error:
            self.broker.get_cancel_backend()

    def __or__(self, other: Message | pipeline) -> pipeline:
        """Combine this group into a pipeline with "other"."""
        return pipeline((self, other))

    def __len__(self) -> int:
        """Returns the size of the group."""
        return len(self.children)

    def __str__(self) -> str:  # pragma: no cover
        return f"group({', '.join(str(child) for child in self.children)})"

    def build(self, options=None) -> list[Message]:
        """Build group for pipeline"""
        if options is None:
            options = {}
        else:
            self.broker.emit_before("build_group_pipeline", group_id=self.group_id, message_ids=list(self.message_ids))

        composition_id = options.get("composition_id", self.group_id)
        cancel_on_error = options.get("cancel_on_error", self.cancel_on_error)
        options = {
            "group_info": self.info.asdict(),
            "composition_id": composition_id,
            "cancel_on_error": self.cancel_on_error,
            **options,
        }
        messages: list[Message] = []
        for group_child in self.children:
            if isinstance(group_child, _BasePipeline):
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
            if isinstance(child, _BasePipeline):
                yield list(child.message_ids)
            else:
                yield child.message_id

    def run(self, *, delay: int | None = None, transaction: bool | None = None) -> Self:
        """Run the actors in this group.

        Parameters:
          delay(int): The minimum amount of time, in milliseconds,
            each message in the group should be delayed by.
          transaction(bool):
        """
        transaction = transaction if transaction is not None else self.broker.group_transaction
        with self.broker.tx() if transaction else nullcontext():
            self.broker.enqueue_many(self.build(), delay=delay)

        return self

    @property
    def results(self) -> CollectionResults[ResultsT]:
        """CollectionResults created from this group, used for result related methods"""
        return cast(CollectionResults[ResultsT], CollectionResults(children=[child.result for child in self.children]))

    def cancel(self) -> None:
        """Mark all the children as cancelled"""
        broker = get_broker()
        backend = broker.get_cancel_backend()
        backend.cancel(list(flatten(self.message_ids)))
