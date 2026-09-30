from dataclasses import fields

from flask import Blueprint
from flask_apispec import doc, marshal_with
from marshmallow import (
    Schema,
    ValidationError,
    fields as mm_fields,
    validate,
    validates_schema,
)
from werkzeug.exceptions import NotFound

from remoulade import get_broker
from remoulade.state import State, StateStatusesEnum

from .apispec import validate_schema

messages_bp = Blueprint("messages", __name__, url_prefix="/messages")


class StatesParamsSchema(Schema):
    """
    Class to validate the state search parameters
    """

    sort_column = mm_fields.Str(
        allow_none=True,
        validate=validate.OneOf([f.name for f in fields(State) if f.name not in {"args", "kwargs", "options"}]),
    )
    sort_direction = mm_fields.Str(allow_none=True, validate=validate.OneOf(["asc", "desc"]))
    size = mm_fields.Int(allow_none=True, validate=validate.Range(min=1, max=1000))
    offset = mm_fields.Int(load_default=0)
    selected_actors = mm_fields.List(mm_fields.String, allow_none=True)
    selected_statuses = mm_fields.List(
        mm_fields.String(validate=validate.OneOf([status.name for status in StateStatusesEnum])),
        allow_none=True,
    )
    selected_message_ids = mm_fields.List(mm_fields.String, allow_none=True)
    selected_composition_ids = mm_fields.List(mm_fields.String, allow_none=True)
    start_datetime = mm_fields.DateTime(allow_none=True)
    end_datetime = mm_fields.DateTime(allow_none=True)

    @validates_schema
    def validate_sort(self, data, **kwargs):
        if data.get("sort_direction") and data.get("sort_column") is None:
            raise ValidationError("sort_column is not defined")


class StateSchema(Schema):
    message_id = mm_fields.Str()
    status = mm_fields.Str(allow_none=True)
    actor_name = mm_fields.Str(allow_none=True)
    args = mm_fields.List(mm_fields.Raw(), allow_none=True)
    kwargs = mm_fields.Dict(keys=mm_fields.Str(), values=mm_fields.Raw(), allow_none=True)
    options = mm_fields.Dict(keys=mm_fields.Str(), values=mm_fields.Raw(), allow_none=True)
    progress = mm_fields.Float(allow_none=True)
    priority = mm_fields.Int(allow_none=True)
    enqueued_datetime = mm_fields.Str(allow_none=True)
    started_datetime = mm_fields.Str(allow_none=True)
    end_datetime = mm_fields.Str(allow_none=True)
    queue_name = mm_fields.Str(allow_none=True)
    composition_id = mm_fields.Str(allow_none=True)


class StatesResponseSchema(Schema):
    data = mm_fields.List(mm_fields.Nested(StateSchema))
    count = mm_fields.Int()


@messages_bp.route("/states", methods=["POST"])
@doc(tags=["state"])
@marshal_with(StatesResponseSchema)
@validate_schema(StatesParamsSchema)
def get_states(**kwargs):
    backend = get_broker().get_state_backend()
    data = [state.as_dict() for state in backend.get_states(**kwargs)]
    StatesResponseSchema().load({"data": data, "count": backend.get_states_count(**kwargs)})
    return {"data": data, "count": backend.get_states_count(**kwargs)}


@messages_bp.route("/states/<message_id>")
@doc(tags=["state"])
@marshal_with(StateSchema)
def get_state(message_id):
    backend = get_broker().get_state_backend()
    data = backend.get_state(message_id)
    if data is None:
        raise NotFound(f"message_id = {message_id} does not exist")
    return data.as_dict()


messages_routes = [get_states, get_state]
