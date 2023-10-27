import datetime
import json
from enum import Enum
from typing import Optional, Tuple, Union

import pytest
from _decimal import Decimal
from pydantic import BaseModel, ValidationError

import remoulade
from remoulade import ActorNotFound, Message
from remoulade.encoder import JSONEncoder, MessageData, PydanticEncoder
from remoulade.results import Results
from remoulade.results.backend import BackendResult


def test_set_encoder_sets_the_global_encoder(pickle_encoder):
    # Given that I've set a Pickle encoder as the global encoder
    # When I get the global encoder
    encoder = remoulade.get_encoder()

    # Then it should be the same as the encoder that was set
    assert encoder == pickle_encoder


def test_pickle_encoder(pickle_encoder, stub_broker, stub_worker):
    # Given that I've set a Pickle encoder as the global encoder
    # And I have an actor that adds a value to a db
    db = []

    @remoulade.actor
    def add_value(x):
        db.append(x)

    # And this actor is declared
    stub_broker.declare_actor(add_value)

    # When I send that actor a message
    add_value.send(1)

    # And wait on the broker and worker
    stub_broker.join(add_value.queue_name)
    stub_worker.join()

    # Then I expect the message to have been processed
    assert db == [1]


class MyEnum(Enum):
    val = "val"
    other = "other"


class MyFirstSchema(BaseModel):
    name: str
    address: Optional[str]
    birth_date: datetime.datetime
    enum: MyEnum


class MySecondSchema(BaseModel):
    id: str
    value: Decimal


class FirstOutputSchema(BaseModel):
    output: Union[str, int]
    error: Union[str, None] = None


class SecondOutputSchema(BaseModel):
    val: str


def tuple_to_list(dict_value: dict, key: str) -> dict:
    return {**dict_value, key: [args for args in dict_value[key]]}


@remoulade.actor(store_results=True)
def my_actor_tuple(
    input_1: MyFirstSchema,
    input_2: Optional[MySecondSchema] = None,
    input_3: Union[MyFirstSchema, MySecondSchema, None] = None,
) -> Tuple[FirstOutputSchema, Optional[SecondOutputSchema]]:
    return FirstOutputSchema(output=input_1.name), SecondOutputSchema(val=input_2.id if input_2 is not None else "2")


@remoulade.actor(store_results=True)
def my_actor(
    input_1: MyFirstSchema,
    input_2: Optional[MySecondSchema] = None,
    input_3: Union[MyFirstSchema, MySecondSchema, None] = None,
) -> FirstOutputSchema:
    return FirstOutputSchema(output=input_1.name)


@remoulade.actor(store_results=True)
def my_actor_none(
    input_1: MyFirstSchema,
    input_2: Optional[MySecondSchema] = None,
    input_3: Union[MyFirstSchema, MySecondSchema, None] = None,
) -> None:
    return


@pytest.fixture
def input_1() -> MyFirstSchema:
    return MyFirstSchema(name="aa", address=None, birth_date=datetime.datetime(2022, 1, 1), enum=MyEnum.val)


@pytest.fixture
def input_2() -> MySecondSchema:
    return MySecondSchema(id="33", value=Decimal("2"))


@pytest.fixture
def message_data_decoded(input_1: MyFirstSchema, input_2: MySecondSchema) -> MessageData:
    return Message(
        queue_name="default",
        actor_name="my_actor_tuple",
        args=(input_1,),
        kwargs={"input_2": input_2},
        options={},
        message_id="dce31cca-70eb-4a55-9b16-5fb6cdd43415",
        message_timestamp=68139918115,
    ).asdict()


@pytest.fixture
def message_data_encoded() -> bytes:
    return (
        b'{"queue_name": "default", '
        b'"actor_name": "my_actor_tuple", '
        b'"args": [{"name": "aa", "address": null, "birth_date": "2022-01-01T00:00:00", "enum": "val"}], '
        b'"kwargs": {"input_2": {"id": "33", "value": "2"}}, '
        b'"options": {}, '
        b'"message_id": "dce31cca-70eb-4a55-9b16-5fb6cdd43415", '
        b'"message_timestamp": 68139918115}'
    )


@pytest.fixture
def encoder(stub_broker, stub_worker, result_backend) -> PydanticEncoder:
    stub_broker.add_middleware(Results(backend=result_backend))
    stub_broker.declare_actor(my_actor_tuple)
    stub_broker.declare_actor(my_actor_none)
    stub_broker.declare_actor(my_actor)
    return PydanticEncoder()


@pytest.fixture
def encoder_with_fallback(stub_broker, stub_worker, result_backend) -> PydanticEncoder:
    stub_broker.add_middleware(Results(backend=result_backend))
    stub_broker.declare_actor(my_actor_tuple)
    return PydanticEncoder(fallback_encoder=JSONEncoder())


def test_encoder_message(encoder: PydanticEncoder, message_data_decoded: MessageData, message_data_encoded: bytes):
    encoded_result = encoder.encode(message_data_decoded)
    assert encoded_result == message_data_encoded

    decoded_result = encoder.decode(message_data_encoded)

    # Args tuple are assumed to become list in remoulade
    assert decoded_result == tuple_to_list(message_data_decoded, "args")

    assert encoder.decode(encoder.encode(message_data_decoded)) == tuple_to_list(message_data_decoded, "args")
    assert encoder.encode(encoder.decode(message_data_encoded)) == message_data_encoded


def test_message_unknown_actor(encoder: PydanticEncoder, message_data_encoded: bytes):
    message_json_decoded = json.loads(message_data_encoded.decode("utf-8"))
    message_json_decoded["actor_name"] = "titi"
    with pytest.raises(ActorNotFound):
        encoder.decode(json.dumps(message_json_decoded).encode("utf-8"))


def test_message_fallback_no_actor_name(encoder_with_fallback: PydanticEncoder, message_data_encoded: bytes):
    message_json_decoded = json.loads(message_data_encoded.decode("utf-8"))
    message_json_decoded["actor_name"] = "titi"
    decoded_result = encoder_with_fallback.decode(json.dumps(message_json_decoded).encode("utf-8"))
    # Do not raise and keep dict instead of schema
    assert decoded_result == tuple_to_list(message_json_decoded, "args")


def test_message_schema_not_matching(encoder: PydanticEncoder, message_data_encoded: bytes):
    message_json_decoded = json.loads(message_data_encoded.decode("utf-8"))
    message_json_decoded["args"] = [{"toto": "a"}]
    message_json_decoded["kwargs"]["input_2"] = {"val": "aaa"}
    with pytest.raises(ValidationError):
        encoder.decode(json.dumps(message_json_decoded).encode("utf-8"))


@pytest.fixture
def backend_result_decoded(input_1: MyFirstSchema, input_2: MySecondSchema) -> MessageData:
    return BackendResult(result=my_actor(input_1, input_2), error=None, forgot=True, actor_name="my_actor").asdict()


@pytest.fixture
def backend_result_decoded_tuple(input_1: MyFirstSchema, input_2: MySecondSchema) -> MessageData:
    return BackendResult(
        result=my_actor_tuple(input_1, input_2), error=None, forgot=True, actor_name="my_actor_tuple"
    ).asdict()


@pytest.fixture
def backend_result_decoded_none(input_1: MyFirstSchema, input_2: MySecondSchema) -> MessageData:
    return BackendResult(result=None, error=None, forgot=True, actor_name="my_actor_none").asdict()


@pytest.fixture
def backend_result_decoded_raise(input_1: MyFirstSchema, input_2: MySecondSchema) -> MessageData:
    return BackendResult(result=None, error="Exception", forgot=True, actor_name="my_actor").asdict()


@pytest.fixture
def backend_result_encoded_raise() -> bytes:
    return b'{"result": null, "error": "Exception", "forgot": true, "actor_name": "my_actor"}'


@pytest.fixture
def backend_result_encoded() -> bytes:
    return b'{"result": {"output": "aa", "error": null}, ' b'"error": null, "forgot": true, "actor_name": "my_actor"}'


@pytest.fixture
def backend_result_encoded_tuple() -> bytes:
    return (
        b'{"result": [{"output": "aa", "error": null}, {"val": "33"}], '
        b'"error": null, "forgot": true, "actor_name": "my_actor_tuple"}'
    )


@pytest.fixture
def backend_result_encoded_none() -> bytes:
    return b'{"result": null, "error": null, "forgot": true, "actor_name": "my_actor_none"}'


def test_encoder_result(encoder: PydanticEncoder, backend_result_decoded: MessageData, backend_result_encoded: bytes):
    encoded_value = encoder.encode(backend_result_decoded)
    assert encoded_value == backend_result_encoded

    decoded_result = encoder.decode(backend_result_encoded)

    assert decoded_result == backend_result_decoded

    assert encoder.decode(encoder.encode(backend_result_decoded)) == backend_result_decoded
    assert encoder.encode(encoder.decode(backend_result_encoded)) == backend_result_encoded


def test_encoder_result_when_raise(
    encoder: PydanticEncoder, backend_result_decoded_raise: MessageData, backend_result_encoded_raise: bytes
):
    encoded_value = encoder.encode(backend_result_decoded_raise)
    assert encoded_value == backend_result_encoded_raise

    decoded_result = encoder.decode(backend_result_encoded_raise)

    assert decoded_result == backend_result_decoded_raise

    assert encoder.decode(encoder.encode(backend_result_decoded_raise)) == backend_result_decoded_raise
    assert encoder.encode(encoder.decode(backend_result_encoded_raise)) == backend_result_encoded_raise


def test_encoder_result_tuple(
    encoder: PydanticEncoder, backend_result_decoded_tuple: MessageData, backend_result_encoded_tuple: bytes
):
    encoded_value = encoder.encode(backend_result_decoded_tuple)
    assert encoded_value == backend_result_encoded_tuple

    decoded_result = encoder.decode(backend_result_encoded_tuple)

    assert decoded_result == backend_result_decoded_tuple

    assert encoder.decode(encoder.encode(backend_result_decoded_tuple)) == backend_result_decoded_tuple
    assert encoder.encode(encoder.decode(backend_result_encoded_tuple)) == backend_result_encoded_tuple


def test_encoder_result_with_none(
    encoder: PydanticEncoder, backend_result_decoded_none: MessageData, backend_result_encoded_none: bytes
):
    encoded_value = encoder.encode(backend_result_decoded_none)
    assert encoded_value == backend_result_encoded_none

    decoded_result = encoder.decode(backend_result_encoded_none)

    assert decoded_result == backend_result_decoded_none

    assert encoder.decode(encoder.encode(backend_result_decoded_none)) == backend_result_decoded_none
    assert encoder.encode(encoder.decode(backend_result_encoded_none)) == backend_result_encoded_none


def test_backend_result_unknown_actor(encoder: PydanticEncoder, backend_result_encoded_tuple: bytes):
    backend_result_json_decoded = json.loads(backend_result_encoded_tuple.decode("utf-8"))
    backend_result_json_decoded["actor_name"] = "titi"
    with pytest.raises(ActorNotFound):
        encoder.decode(json.dumps(backend_result_json_decoded).encode("utf-8"))


def test_backend_result_schema_not_matching(encoder: PydanticEncoder, backend_result_encoded_tuple: bytes):
    backend_result_json_decoded = json.loads(backend_result_encoded_tuple.decode("utf-8"))
    backend_result_json_decoded["result"] = [{"val": "titi"}]
    with pytest.raises(ValidationError):
        encoder.decode(json.dumps(backend_result_json_decoded).encode("utf-8"))


def test_fallback_no_schema(encoder_with_fallback: PydanticEncoder, backend_result_encoded_tuple: bytes):
    backend_result_json_decoded = json.loads(backend_result_encoded_tuple.decode("utf-8"))
    backend_result_json_decoded["result"] = [{"val": "titi"}]
    decoded_backend_result = encoder_with_fallback.decode(json.dumps(backend_result_json_decoded).encode("utf-8"))
    # Do not raise and keep dict instead of schema
    assert decoded_backend_result == backend_result_json_decoded
