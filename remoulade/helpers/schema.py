import re
from inspect import signature

from marshmallow import Schema, fields


def get_schema(actor):
    params = signature(actor.fn).parameters
    args = {}
    for el in params:
        args[params[el].name] = parse_annotation_to_field(str(params[el].annotation))
    schema = Schema.from_dict(args)()
    return schema


def parse_annotation_to_field(annotation):
    if annotation == "str" or annotation == "<class 'str'>":
        return fields.Str()
    if annotation == "int" or annotation == "<class 'int'>":
        return fields.Int()
    if annotation == "float" or annotation == "<class 'float'>":
        return fields.Float()
    if annotation == "bool" or annotation == "<class 'bool'>":
        return fields.Bool()
    if annotation == "datetime.datetime" or annotation == "<class 'datetime.datetime'>":
        return fields.DateTime()
    if annotation == "datetime.date" or annotation == "<class 'datetime.date'>":
        return fields.Date()
    if annotation == "list" or annotation == "<class 'list'>":
        return fields.List(fields.Raw())
    if annotation == "dict" or annotation == "<class 'dict'>":
        return fields.Dict(fields.Raw, fields.Raw)
    if re.compile("typing.List").match(annotation):
        list_type = annotation[annotation.find("[") + 1: annotation.rfind("]")]
        return fields.List(parse_annotation_to_field(list_type))
    if re.compile("typing.Dict").match(annotation):
        return fields.Dict(
            parse_annotation_to_field(annotation[annotation.find("[") + 1: annotation.find(",")]),
            annotation[annotation.find(",") + 2: annotation.rfind("]")],
        )
    return fields.Raw()
