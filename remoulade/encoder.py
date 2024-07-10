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
from typing import Any, Dict, Optional, get_type_hints

from typing_extensions import Annotated

try:
    from pydantic import BaseModel, TypeAdapter, WithJsonSchema
    from simplejson.decoder import JSONDecoder
    from simplejson.encoder import JSONEncoder as _JSONEncoder
except ImportError:  # pragma: no cover
    warnings.warn(
        "Pydantic and simplejson are not available.  Run `pip install remoulade[pydantic]`",
        ImportWarning,
        stacklevel=2,
    )


#: Represents the contents of a Message object as a dict.
MessageData = Dict[str, Any]


class Encoder(abc.ABC):
    """Base class for message encoders."""

    @abc.abstractmethod
    def encode(self, data: MessageData) -> bytes:  # pragma: no cover
        """Convert message metadata into a bytestring."""
        raise NotImplementedError

    @abc.abstractmethod
    def decode(self, data: bytes) -> MessageData:  # pragma: no cover
        """Convert a bytestring into message metadata."""
        raise NotImplementedError


class JSONEncoder(Encoder):
    """Encodes messages as JSON.  This is the default encoder."""

    def encode(self, data: MessageData) -> bytes:
        return json.dumps(data, separators=(",", ":")).encode("utf-8")

    def decode(self, data: bytes) -> MessageData:
        return json.loads(data.decode("utf-8"))


class PickleEncoder(Encoder):
    """Pickles messages.

    Warning:
      This encoder is not secure against maliciously-constructed data.
      Use it at your own risk.
    """

    encode = pickle.dumps  # type: ignore
    decode = pickle.loads  # type: ignore


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

    def __init__(self, fallback_encoder: Optional[Encoder] = None):
        self.fallback_encoder = fallback_encoder
        self.json_encoder = _JSONEncoder(default=self.default)
        self.json_decoder = JSONDecoder()

    @staticmethod
    def default(o):
        if isinstance(o, BaseModel):
            # keep dict otherwise it will be serialized as a string (see Pydantic .json())
            return json.loads(o.model_dump_json())
        raise TypeError("Object of type %s is not JSON serializable" % o.__class__.__name__)

    def encode(self, data: MessageData) -> bytes:
        try:
            return self.json_encoder.encode(data).encode("utf-8")
        except Exception as e:
            if self.fallback_encoder is not None:
                return self.fallback_encoder.encode(data)
            else:
                raise e

    def decode(self, data: bytes) -> MessageData:
        from remoulade import get_broker

        try:
            raw_message = self.json_decoder.decode(data.decode("utf-8"))
            actor_name = raw_message["actor_name"]
            actor_fn = get_broker().get_actor(actor_name).fn

            # Retrieve the Pydantic schemas from typing
            schemas_by_param_name: Dict[str, "TypeAdapter"] = {}
            for param_name, type_hint in get_type_hints(actor_fn).items():
                schemas_by_param_name[param_name] = TypeAdapter(
                    Annotated[
                        type_hint,
                        WithJsonSchema(
                            {"type": type_hint, "description": f"{param_name}_schema"}, mode="serialization"
                        ),
                    ]
                )

            # Override message_data with Pydantic schema when it matches
            parsed_message: Dict[str, Any] = {}
            for key, values in raw_message.items():
                if key == "kwargs":
                    assert isinstance(values, dict)
                    parsed_message[key] = {
                        param_name: schemas_by_param_name[param_name].validate_python(raw_value)
                        for param_name, raw_value in values.items()
                    }
                elif key == "args":
                    assert isinstance(values, list)
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
        except Exception as e:
            if self.fallback_encoder is not None:
                return self.fallback_encoder.decode(data)
            else:
                raise e
