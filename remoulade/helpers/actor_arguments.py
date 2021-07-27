import re
from inspect import signature, Parameter


def get_actor_arguments(actor):
    params = signature(actor.fn).parameters

    def parsetype(rawtype):
        try:
            if re.match("typing", str(rawtype)):
                return str(rawtype)
            return rawtype.__name__
        except AttributeError:
            return str(rawtype)

    for param in params.values():
        arg = {"name": param.name}
        if param.annotation != Parameter.empty:
            arg["type"] = parsetype(param.annotation)
        if param.default != Parameter.empty:
            arg["default"] = str(param.default)

    return [
        {"name": param.name, "type": parsetype(param.annotation), "default": str(param.default)}
        for param in params.values()
    ]
