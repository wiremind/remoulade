from marshmallow import Schema, ValidationError, fields, validate, validates_schema


class MessageSchema(Schema):
    """
        Class to validate post data in /messages
    """

    actor_name = fields.Str(validate=validate.Length(min=1), required=True)
    args = fields.List(fields.Raw(), allow_none=True)
    kwargs = fields.Dict(allow_none=True)
    options = fields.Dict(allow_none=True)
    delay = fields.Number(validate=validate.Range(min=1), allow_none=True)


class PageSchema(Schema):
    """
        Class to validate the attributes of a page
    """

    sort_column = fields.Str(allow_none=True)
    sort_direction = fields.Str(allow_none=True, validate=validate.OneOf(["asc", "desc"]))
    size = fields.Int(missing=50, validate=validate.Range(min=1, max=1000))
    search_value = fields.Str(allow_none=True)
    offset = fields.Int(missing=0)

    @validates_schema
    def validate_sort(self, data, **kwargs):
        if data.get("sort_direction") and data.get("sort_column") is None:
            raise ValidationError("sort_column is not defined")
