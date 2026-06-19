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

import abc
import json
import pickle
import warnings
from typing import Annotated, Any, get_type_hints, override

from remoulade.errors import UnsupportedMessageEncoding

try:
    from pydantic import TypeAdapter, WithJsonSchema
except ImportError:  # pragma: no cover
    warnings.warn(
        "Pydantic is not available.  Run `pip install remoulade[pydantic]`",
        ImportWarning,
        stacklevel=2,
    )


#: Represents the contents of a Message object as a dict.
MessageData = dict[str, Any]
JsonData = dict[str, Any]


class Encoder(abc.ABC):
    """Base class for message encoders."""

    @abc.abstractmethod
    def encode_in_bytes(self, data: MessageData) -> bytes:
        raise NotImplementedError

    @abc.abstractmethod
    def decode_bytes(self, data: bytes) -> MessageData:
        raise NotImplementedError

    def encode_in_json(self, data: MessageData) -> JsonData:
        encoded = self._encode_in_json(data)
        try:
            json.dumps(encoded)
        except (TypeError, ValueError) as e:
            raise UnsupportedMessageEncoding("This is not a valid json") from e
        return encoded

    @abc.abstractmethod
    def _encode_in_json(self, data: MessageData) -> JsonData:
        raise NotImplementedError

    @abc.abstractmethod
    def decode_json(self, data: JsonData) -> MessageData:
        raise NotImplementedError


class JSONEncoder(Encoder):
    """Encodes messages as JSON.  This is the default encoder."""

    @override
    def encode_in_bytes(self, data: MessageData) -> bytes:
        """Convert message metadata into a bytestring."""
        # Serialize directly: routing through encode_in_json would add a
        # throwaway json.dumps validation pass on top of this one.
        return json.dumps(data, separators=(",", ":")).encode("utf-8")

    @override
    def decode_bytes(self, data: bytes) -> MessageData:
        """Convert a bytestring into message metadata."""
        return self.decode_json(json.loads(data.decode("utf-8")))

    @override
    def _encode_in_json(self, data: MessageData) -> JsonData:
        return data

    @override
    def decode_json(self, data: JsonData) -> MessageData:
        return data


class PickleEncoder(Encoder):
    """Pickles messages.

    Warning:
      This encoder is not secure against maliciously-constructed data.
      Use it at your own risk.
    """

    @override
    def encode_in_bytes(self, data: MessageData) -> bytes:
        return pickle.dumps(data)

    @override
    def decode_bytes(self, data: bytes) -> MessageData:
        return pickle.loads(data)  # noqa: S301

    @override
    def _encode_in_json(self, data: MessageData) -> JsonData:
        raise TypeError("PickleEncoder does not support JSON encoding.")

    @override
    def decode_json(self, data: JsonData) -> MessageData:
        raise TypeError("PickleEncoder does not support JSON decoding.")


class PydanticEncoder(Encoder):
    """PydanticEncoder remoulade encoder working with Pydantic schemas (install remoulade[pydantic] extra dependency)
    With this encoder you must use only Pydantic schema as inputs/outputs of the actors and type them explicitly.

    class MyActorInputSchema(BaseModel):
        ...

    class MyActorOutputSchema(BaseModel):
        ...

    @remoulade.actor()
    def my_actor(input_1: MyActorInputSchema, input_2: MyActorInputSchema | None = None) -> MyActorOutputSchema:
        ...
        return MyActorOutputSchema()
    """

    def __init__(self):
        self.json_adapter = TypeAdapter(object)

    @override
    def encode_in_bytes(self, data: MessageData) -> bytes:
        return json.dumps(self._encode_in_json(data)).encode("utf-8")

    @override
    def decode_bytes(self, data: bytes) -> MessageData:
        return self.decode_json(json.loads(data.decode("utf-8")))

    @override
    def _encode_in_json(self, data: MessageData) -> JsonData:
        return self.json_adapter.dump_python(data, mode="json")

    @override
    def decode_json(self, data: JsonData) -> MessageData:
        from remoulade import get_broker

        actor_name = data["actor_name"]
        actor_fn = get_broker().get_actor(actor_name).fn

        # Retrieve the Pydantic schemas from typing
        schemas_by_param_name: dict[str, TypeAdapter] = {}
        for param_name, type_hint in get_type_hints(actor_fn).items():
            schemas_by_param_name[param_name] = TypeAdapter(
                Annotated[
                    type_hint,
                    WithJsonSchema({"type": type_hint, "description": f"{param_name}_schema"}, mode="serialization"),
                ]
            )

        # Override message_data with Pydantic schema when it matches.
        parsed_message: dict[str, Any] = {}
        for key, values in data.items():
            if key == "kwargs":
                if not isinstance(values, dict):
                    raise TypeError(f"Expected `values` to be a dict, got {type(values).__name__}")
                parsed_message[key] = {
                    param_name: schemas_by_param_name[param_name].validate_python(raw_value)
                    for param_name, raw_value in values.items()
                }
            elif key == "args":
                if not isinstance(values, list):
                    raise TypeError(f"Expected `values` to be a list, got {type(values).__name__}")
                schemas = list(schemas_by_param_name.values())
                parsed_message[key] = [
                    schemas[order].validate_python(raw_value) for order, raw_value in enumerate(values)
                ]
            elif key == "result":
                if values is None:
                    parsed_message[key] = None
                else:
                    parsed_message[key] = schemas_by_param_name["return"].validate_python(values)
            else:
                parsed_message[key] = values

        return parsed_message
