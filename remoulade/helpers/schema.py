import datetime
import re
from inspect import signature

from marshmallow import Schema, fields


def get_schema(actor):
    params = signature(actor.fn).parameters
    args = {}
    for el in params:
        args[params[el].name] = parse_annotation_to_field(params[el].annotation)
    schema = Schema.from_dict(args)()
    return schema


def parse_annotation_to_field(annotation):
    if annotation == str or annotation == "str":
        return fields.Str()
    if annotation == int or annotation == "int":
        return fields.Int()
    if annotation == float or annotation == "float":
        return fields.Float()
    if annotation == bool or annotation == "bool":
        return fields.Bool()
    if annotation == datetime.datetime or annotation == "datetime.datetime":
        return fields.DateTime()
    if annotation == datetime.date or annotation == "datetime.date":
        return fields.Date()
    if annotation == list or annotation == "list":
        return fields.List(fields.Raw())
    if annotation == dict or annotation == "dict":
        return fields.Dict(fields.Raw, fields.Raw)
    if re.compile("typing.List").match(str(annotation)):
        return fields.List(parse_annotation_to_field(annotation.__args__[0]))
    if re.compile("typing.Dict").match(str(annotation)):
        return fields.Dict(
            parse_annotation_to_field(annotation.__args__[0]),
            parse_annotation_to_field(annotation.__args__[1]),
        )
    return fields.Raw()
