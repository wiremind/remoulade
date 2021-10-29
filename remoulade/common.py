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
from collections.abc import Iterable
from itertools import islice
from time import time
from typing import Iterator, List


def current_millis():
    """Returns the current UNIX time in milliseconds."""
    return int(time() * 1000)


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
