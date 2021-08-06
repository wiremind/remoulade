from marshmallow import Schema, ValidationError, fields, validate, validates_schema

from remoulade.state import StateStatusesEnum


class MessageSchema(Schema):
    """
    Class to validate post data in /messages
    """

    actor_name = fields.Str(validate=validate.Length(min=1), required=True)
    args = fields.List(fields.Raw(), allow_none=True)
    kwargs = fields.Dict(allow_none=True)
    options = fields.Dict(allow_none=True)
    delay = fields.Number(validate=validate.Range(min=1), allow_none=True)


class DeleteSchema(Schema):
    """
    Class to validate delete body data in /messages/states
    """

    max_age = fields.Int(allow_none=True)
    not_started = fields.Bool(missing=False)


class PageSchema(Schema):
    """
    Class to validate the attributes of a page
    """

    sort_column = fields.Str(allow_none=True)
    sort_direction = fields.Str(allow_none=True, validate=validate.OneOf(["asc", "desc"]))
    size = fields.Int(missing=None, validate=validate.Range(min=1, max=1000))
    offset = fields.Int(missing=0)
    selected_actors = fields.List(fields.String, allow_none=True)
    selected_statuses = fields.List(
        fields.String(validate=validate.ContainsNoneOf([status.name for status in StateStatusesEnum])),
        allow_none=True,
    )
    selected_ids = fields.List(fields.String, allow_none=True)
    start_datetime = fields.DateTime(allow_none=True)
    end_datetime = fields.DateTime(allow_none=True)

    @validates_schema
    def validate_sort(self, data, **kwargs):
        if data.get("sort_direction") and data.get("sort_column") is None:
            raise ValidationError("sort_column is not defined")
