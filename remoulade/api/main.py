""" This file describe the API to get the state of messages """
from flask import Flask, request
from marshmallow import ValidationError
from werkzeug.exceptions import BadRequest, HTTPException, NotFound

import remoulade
from remoulade import get_broker, get_scheduler
from remoulade.errors import NoScheduler, RemouladeError
from remoulade.state import StateNamesEnum

from .schema import MessageSchema

app = Flask(__name__)


@app.route("/messages/states")
def get_states():
    name_state = request.args.get("name")
    backend = remoulade.get_broker().get_state_backend()
    data = [s.as_dict() for s in backend.get_states()]
    if name_state is not None:
        name_state = name_state.lower().capitalize()
        try:
            state = StateNamesEnum(name_state)
            data = [s for s in data if s["name"] == state.value]
        except ValueError:
            raise BadRequest("{} state is not defined.".format(name_state))
    return {"result": data}


@app.route("/messages/state/<message_id>")
def get_state(message_id):
    backend = remoulade.get_broker().get_state_backend()
    data = backend.get_state(message_id)
    if data is None:
        raise NotFound("message_id = {} does not exist".format(message_id))
    return data.as_dict()


@app.route("/messages/cancel/<message_id>", methods=["POST"])
def cancel_message(message_id):
    backend = remoulade.get_broker().get_cancel_backend()
    backend.cancel([message_id])
    return {"result": True}


@app.route("/scheduled/jobs")
def get_scheduled_jobs():
    scheduled_jobs = get_scheduler().get_redis_schedule()
    return {"result": [job.as_dict() for job in scheduled_jobs.values()]}


@app.route("/messages", methods=["POST"])
def enqueue_message():
    payload = MessageSchema().load(request.json)
    actor = get_broker().get_actor(payload.pop("actor_name"))
    options = payload.pop("options") or {}
    actor.send_with_options(**payload, **options)
    return {"result": "ok"}


@app.route("/actors")
def get_actors():
    return {"result": [actor.as_dict() for actor in get_broker().actors.values()]}


@app.errorhandler(RemouladeError)
def remoulade_exception(e):
    return {"error": str(e)}, 500


@app.errorhandler(HTTPException)
def http_exception(e):
    return {"error": str(e)}, e.code


@app.errorhandler(ValidationError)
def validation_error(e):
    return {"error": e.normalized_messages()}, 400


@app.errorhandler(NoScheduler)
def no_scheduler(e):
    return {"result": "No scheduller is set."}
