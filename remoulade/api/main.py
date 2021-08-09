""" This file describe the API to get the state of messages """
import sys
from typing import List

from flask import Flask, request
from marshmallow import Schema, ValidationError, fields, validate
from typing_extensions import TypedDict
from werkzeug.exceptions import HTTPException

from remoulade import get_broker
from remoulade.errors import NoResultBackend, RemouladeError
from remoulade.result import Result
from remoulade.results import ResultMissing

from .scheduler import scheduler_bp
from .state import messages_bp

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


@app.route("/messages/cancel/<message_id>", methods=["POST"])
def cancel_message(message_id):
    backend = get_broker().get_cancel_backend()
    backend.cancel([message_id])
    return {"result": "ok"}


@app.route("/messages/requeue/<message_id>")
def requeue_message(message_id):
    broker = get_broker()
    backend = broker.get_state_backend()
    state = backend.get_state(message_id)
    actor = broker.get_actor(state.actor_name)
    payload = {"args": state.args, "kwargs": state.kwargs}
    pipe_target = state.options.get("pipe_target")
    if pipe_target is None:
        actor.send_with_options(**payload, **state.options)
        return {"result": "ok"}
    else:
        return {"error": "requeue message in a pipeline not supported"}, 400


@app.route("/messages/result/<message_id>")
def get_results(message_id):
    from ..message import get_encoder

    max_size = 1e4
    try:
        result = Result(message_id=message_id).get()
        encoded_result = get_encoder().encode(result).decode("utf-8")
        size_result = sys.getsizeof(encoded_result)
        if size_result >= max_size:
            encoded_result = "The result is too big {}M".format(size_result / 1e6)
        return {"result": encoded_result}
    except ResultMissing:
        return {"result": "result is missing"}
    except NoResultBackend:
        return {"result": "no result backend"}
    except (UnicodeDecodeError, TypeError):
        return {"result": "non serializable result"}


@app.route("/messages", methods=["POST"])
def enqueue_message():
    payload = MessageSchema().load(request.json)
    actor = get_broker().get_actor(payload.pop("actor_name"))
    options = payload.pop("options") or {}
    actor.send_with_options(**payload, **options)
    return {"result": "ok"}


class GroupMessagesT(TypedDict):
    group_id: str
    messages: List[dict]


@app.route("/actors")
def get_actors():
    return {"result": [actor.as_dict() for actor in get_broker().actors.values()]}


@app.route("/options")
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
