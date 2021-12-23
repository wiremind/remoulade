""" This file describe the API to get the state of messages """
import sys
from typing import List

from flask import Flask
from flask_apispec import marshal_with
from marshmallow import Schema, ValidationError, fields, validate, validates_schema
from typing_extensions import TypedDict
from werkzeug.exceptions import HTTPException

import remoulade
from remoulade import get_broker
from remoulade.errors import NoResultBackend, NoStateBackend, RemouladeError
from remoulade.result import Result
from remoulade.results import ResultMissing

from .apispec import add_swagger, validate_schema
from .scheduler import scheduler_bp, scheduler_routes
from .state import messages_bp, messages_routes

app = Flask(__name__)
app.register_blueprint(scheduler_bp)
app.register_blueprint(messages_bp)


class MessageSchema(Schema):
    """
    Class to validate post data in /messages
    """

    actor_name = fields.Str(validate=validate.Length(min=1), required=True)
    args = fields.List(fields.Raw(), allow_none=True)
    kwargs = fields.Dict(allow_none=True)
    options = fields.Dict(allow_none=True)
    delay = fields.Number(validate=validate.Range(min=1), allow_none=True)

    @validates_schema
    def validate_actor_name(self, data, **kwargs):
        actor_name = data.get("actor_name")
        if actor_name not in remoulade.get_broker().actors.keys():
            raise ValidationError(f"No actor named {actor_name} exists")


class ResponseSchema(Schema):
    result = fields.Raw(allow_none=True)
    error = fields.Str(allow_none=True)


class ArgsSchema(Schema):
    name = fields.Str()
    type = fields.Str()
    default = fields.Str(allow_none=True)


class ActorSchema(Schema):
    name = fields.Str()
    queue_name = fields.Str()
    alternative_queues = fields.List(fields.Str())
    priority = fields.Int()
    args = fields.List(fields.Nested(ArgsSchema))


class ActorResponseSchema(Schema):
    result = fields.List(fields.Nested(ActorSchema))


class OptionsResponseSchema(Schema):
    options = fields.List(fields.Str())


@app.route("/messages/cancel/<message_id>", methods=["POST"])
@marshal_with(ResponseSchema)
def cancel_message(message_id):
    broker = get_broker()
    cancel_backend = broker.get_cancel_backend()
    try:
        # If a single message in a composition is canceled, we cancel the whole composition
        state_backend = broker.get_state_backend()
        states_count = state_backend.get_states_count(selected_composition_ids=[message_id])
        state = state_backend.get_state(message_id)
        if states_count == 0 and state is None:
            raise ValidationError(f"This message id or composition id {message_id} does not exist.")
        if state and state.composition_id:
            message_id = state.composition_id
    except NoStateBackend:
        pass
    cancel_backend.cancel([message_id])
    return {"result": "ok"}


@app.route("/messages/requeue/<message_id>", methods=["POST"])
@marshal_with(ResponseSchema)
def requeue_message(message_id):
    broker = get_broker()
    backend = broker.get_state_backend()
    state = backend.get_state(message_id)
    if state is None:
        raise ValidationError(f"No message with id {message_id} exists")
    actor = broker.get_actor(state.actor_name)
    payload = {"args": state.args, "kwargs": state.kwargs}
    pipe_target = state.options.get("pipe_target")
    if pipe_target is None:
        actor.send_with_options(**payload, **state.options)
        return {"result": "ok"}
    else:
        return {"error": "requeue message in a pipeline not supported"}, 400


@app.route("/messages/result/<message_id>")
@marshal_with(ResponseSchema)
def get_results(message_id):
    from ..message import get_encoder

    max_size = 1e4
    try:
        result = Result(message_id=message_id).get()
        encoded_result = get_encoder().encode(result).decode("utf-8")
        size_result = sys.getsizeof(encoded_result)
        if size_result >= max_size:
            encoded_result = f"The result is too big {size_result / 1e6}M"
        return {"result": encoded_result}
    except ResultMissing:
        return {"result": "result is missing"}
    except NoResultBackend:
        return {"result": "no result backend"}
    except (UnicodeDecodeError, TypeError):
        return {"result": "non serializable result"}


@app.route("/messages", methods=["POST"])
@marshal_with(ResponseSchema)
@validate_schema(MessageSchema)
def enqueue_message(**kwargs):
    actor = get_broker().get_actor(kwargs.pop("actor_name"))
    options = kwargs.pop("options") or {}
    actor.send_with_options(**kwargs, **options)
    return {"result": "ok"}


class GroupMessagesT(TypedDict):
    group_id: str
    messages: List[dict]


@app.route("/actors")
@marshal_with(ActorResponseSchema)
def get_actors():
    return {"result": [actor.as_dict() for actor in get_broker().actors.values()]}


@app.route("/options")
@marshal_with(OptionsResponseSchema)
def get_options():
    broker = get_broker()
    return {"options": list(broker.actor_options)}


@app.errorhandler(RemouladeError)
def remoulade_exception(e):
    return {"error": str(e)}, 500


@app.errorhandler(HTTPException)
def http_exception(e):
    return {"error": str(e)}, e.code


@app.errorhandler(ValidationError)
def validation_error(e):
    return {"error": e.normalized_messages()}, 400


routes = [cancel_message, requeue_message, get_results, enqueue_message, get_actors, get_options]

add_swagger(app, {"main": routes, "scheduler": scheduler_routes, "messages": messages_routes})
