from flask import Blueprint, request
from marshmallow import Schema, ValidationError, fields, validate, validates_schema
from werkzeug.exceptions import NotFound

from remoulade import get_broker
from remoulade.state import State, StateStatusesEnum
from remoulade.state.backends import PostgresBackend

messages_bp = Blueprint("messages", __name__, url_prefix="/messages")


class DeleteSchema(Schema):
    """
    Class to validate delete body data in /messages/states
    """

    max_age = fields.Int(allow_none=True)
    not_started = fields.Bool(missing=False)


class StatesSchema(Schema):
    """
    Class to validate the state search parameters
    """

    sort_column = fields.Str(allow_none=True, validate=validate.OneOf(State._fields))
    sort_direction = fields.Str(allow_none=True, validate=validate.OneOf(["asc", "desc"]))
    size = fields.Int(allow_none=True, validate=validate.Range(min=1, max=1000))
    offset = fields.Int(missing=0)
    selected_actors = fields.List(fields.String, allow_none=True)
    selected_statuses = fields.List(
        fields.String(validate=validate.OneOf([status.name for status in StateStatusesEnum])),
        allow_none=True,
    )
    selected_ids = fields.List(fields.String, allow_none=True)
    start_datetime = fields.DateTime(allow_none=True)
    end_datetime = fields.DateTime(allow_none=True)

    @validates_schema
    def validate_sort(self, data, **kwargs):
        if data.get("sort_direction") and data.get("sort_column") is None:
            raise ValidationError("sort_column is not defined")


@messages_bp.route("/states", methods=["POST"])
def get_states():
    states_kwargs = StatesSchema().load(request.json or {})
    backend = get_broker().get_state_backend()
    data = [state.as_dict() for state in backend.get_states(**states_kwargs)]
    return {"data": data, "count": backend.get_states_count(**states_kwargs)}


@messages_bp.route("/states", methods=["DELETE"])
def clean_states():
    backend = get_broker().get_state_backend()
    if not isinstance(backend, PostgresBackend):
        return {"error": "deleting states is only supported by the PostgresBackend"}, 400
    states_kwargs = DeleteSchema().load(request.json or {})
    get_broker().get_state_backend().clean(**states_kwargs)
    return {"result": "ok"}


@messages_bp.route("/states/<message_id>")
def get_state(message_id):
    backend = get_broker().get_state_backend()
    data = backend.get_state(message_id)
    if data is None:
        raise NotFound("message_id = {} does not exist".format(message_id))
    return data.as_dict()
