from marshmallow import Schema, fields, validate


class MessageSchema(Schema):
    """
        Class to validate post data in /messages
    """

    actor_name = fields.Str(validate=validate.Length(min=1), required=True)
    args = fields.List(fields.Raw())
    kwargs = fields.Dict()
    options = fields.Dict()
    delay = fields.Number(validate=validate.Range(min=1))
