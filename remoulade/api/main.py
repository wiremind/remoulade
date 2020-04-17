""" This file describe the API to get the state of messages """
from flask import Flask, request
from werkzeug.exceptions import BadRequest, HTTPException, NotFound

import remoulade
from remoulade.state import StateNamesEnum

app = Flask(__name__)


@app.route("/messages/states")
def get_states():
    name_state = request.args.get("name")
    backend = remoulade.get_broker().get_state_backend()
    data = [s.asdict() for s in backend.get_states()]
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
    return data.asdict()


@app.errorhandler(HTTPException)
def http_exception(e):
    return {"error": str(e)}, e.code
