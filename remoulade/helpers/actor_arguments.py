import re
from inspect import Parameter, signature


def get_actor_arguments(actor):
    params = signature(actor.fn).parameters

    def parsetype(rawtype):
        try:
            if re.match("typing", str(rawtype)):
                return str(rawtype)
            return str(rawtype.__name__)
        except AttributeError:
            return str(rawtype)

    args = []
    for param in params.values():
        arg = {"name": param.name}
        if param.annotation != Parameter.empty:
            arg["type"] = parsetype(param.annotation)
        if param.default != Parameter.empty:
            arg["default"] = str(param.default)
        args.append(arg)

    return args
