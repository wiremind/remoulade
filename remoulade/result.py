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
from typing import Any, Generic, Optional, TypeVar, Union, overload

import attr
from typing_extensions import Literal

from remoulade.results.errors import ErrorStored

from .broker import get_broker
from .results import ResultBackend

R = TypeVar("R", covariant=True)


@attr.s(frozen=True, slots=True, kw_only=True, auto_attribs=True)
class Result(Generic[R]):
    """Encapsulates metadata needed to retrieve the result of a message

    Parameters:
      message_id(str): The id of the message sent to the broker.
    """

    message_id: str

    def asdict(self):
        return attr.asdict(self)

    @overload
    def get(
        self,
        *,
        block: bool = False,
        timeout: Optional[int] = None,
        raise_on_error: Literal[True] = True,
        forget: bool = False,
    ) -> R:
        ...

    @overload
    def get(
        self, *, block: bool = False, timeout: Optional[int] = None, raise_on_error: bool = True, forget: bool = False
    ) -> Union[R, ErrorStored]:
        ...

    def get(
        self, *, block: bool = False, timeout: Optional[int] = None, raise_on_error: bool = True, forget: bool = False
    ) -> Union[R, ErrorStored]:
        """Get the result associated with a message_id from a result backend.

        Parameters:
          block(bool): Whether or not to block while waiting for a
            result.
          timeout(int): The maximum amount of time, in ms, to block
            while waiting for a result.
          raise_on_error(bool): raise an error if the result stored in
            an error
          forget(bool): if true the result is discarded from the result
            backend

        Raises:
          RuntimeError: If there is no result backend on the default
            broker.
          ResultMissing: When block is False and the result isn't set.
          ResultTimeout: When waiting for a result times out.
          ErrorStored: When the result is an error and raise_on_error is True

        Returns:
          object: The result.
        """
        return self.backend.get_result(
            self.message_id, block=block, timeout=timeout, forget=forget, raise_on_error=raise_on_error
        )

    async def async_get(
        self, *, timeout: Optional[int] = None, raise_on_error: bool = True, forget: bool = False
    ) -> Any:
        return await self.backend.async_get_result(
            self.message_id, timeout=timeout, forget=forget, raise_on_error=raise_on_error
        )

    def completed(self) -> bool:
        """Returns True when the job has been completed (error or result).

        Raises:
          RuntimeError: If your broker doesn't have a result backend
            set up.
        """
        return self.backend.get_status([self.message_id]) == 1

    @property
    def backend(self) -> ResultBackend:
        broker = get_broker()
        return broker.get_result_backend()
