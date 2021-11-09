from flask import Blueprint
from flask_apispec import doc, marshal_with
from marshmallow import Schema, ValidationError, fields, validate, validates_schema
from werkzeug.exceptions import NotFound

from remoulade import get_broker
from remoulade.state import State, StateStatusesEnum
from remoulade.state.backends import PostgresBackend

from .apispec import validate_schema

messages_bp = Blueprint("messages", __name__, url_prefix="/messages")


class DeleteSchema(Schema):
    """
    Class to validate delete body data in /messages/states
    """

    max_age = fields.Int(allow_none=True)
    not_started = fields.Bool(missing=False)


class StatesParamsSchema(Schema):
    """
    Class to validate the state search parameters
    """

    sort_column = fields.Str(
        allow_none=True,
        validate=validate.OneOf([field for field in State._fields if field not in ["args", "kwargs", "options"]]),
    )
    sort_direction = fields.Str(allow_none=True, validate=validate.OneOf(["asc", "desc"]))
    size = fields.Int(allow_none=True, validate=validate.Range(min=1, max=1000))
    offset = fields.Int(missing=0)
    selected_actors = fields.List(fields.String, allow_none=True)
    selected_statuses = fields.List(
        fields.String(validate=validate.OneOf([status.name for status in StateStatusesEnum])),
        allow_none=True,
    )
    selected_message_ids = fields.List(fields.String, allow_none=True)
    selected_composition_ids = fields.List(fields.String, allow_none=True)
    start_datetime = fields.DateTime(allow_none=True)
    end_datetime = fields.DateTime(allow_none=True)

    @validates_schema
    def validate_sort(self, data, **kwargs):
        if data.get("sort_direction") and data.get("sort_column") is None:
            raise ValidationError("sort_column is not defined")


class StateSchema(Schema):
    message_id = fields.Str()
    status = fields.Str(allow_none=True)
    actor_name = fields.Str(allow_none=True)
    args = fields.List(fields.Raw(), allow_none=True)
    kwargs = fields.Dict(keys=fields.Str(), values=fields.Raw(), allow_none=True)
    options = fields.Dict(keys=fields.Str(), values=fields.Raw(), allow_none=True)
    progress = fields.Number(allow_none=True)
    priority = fields.Int(allow_none=True)
    enqueued_datetime = fields.Str(allow_none=True)
    started_datetime = fields.Str(allow_none=True)
    end_datetime = fields.Str(allow_none=True)
    queue_name = fields.Str(allow_none=True)
    composition_id = fields.Str(allow_none=True)


class StatesResponseSchema(Schema):
    data = fields.List(fields.Nested(StateSchema))
    count = fields.Int()


@messages_bp.route("/states", methods=["POST"])
@doc(tags=["state"])
@marshal_with(StatesResponseSchema)
@validate_schema(StatesParamsSchema)
def get_states(**kwargs):
    backend = get_broker().get_state_backend()
    data = [state.as_dict() for state in backend.get_states(**kwargs)]
    StatesResponseSchema().load({"data": data, "count": backend.get_states_count(**kwargs)})
    return {"data": data, "count": backend.get_states_count(**kwargs)}


@messages_bp.route("/states", methods=["DELETE"])
@doc(tags=["state"])
@validate_schema(DeleteSchema)
def clean_states(**kwargs):
    backend = get_broker().get_state_backend()
    if not isinstance(backend, PostgresBackend):
        return {"error": "deleting states is only supported by the PostgresBackend"}, 400
    get_broker().get_state_backend().clean(**kwargs)
    return {"result": "ok"}


@messages_bp.route("/states/<message_id>")
@doc(tags=["state"])
@marshal_with(StateSchema)
def get_state(message_id):
    backend = get_broker().get_state_backend()
    data = backend.get_state(message_id)
    if data is None:
        raise NotFound(f"message_id = {message_id} does not exist")
    return data.as_dict()


messages_routes = [get_states, clean_states, get_state]
