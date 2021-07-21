from inspect import Parameter, signature


def get_actor_arguments(actor):
    params = signature(actor.fn).parameters

    def parse_type(rawtype):
        if str(rawtype).startswith("typing"):
            return str(rawtype)
        try:
            return str(rawtype.__name__)
        except AttributeError:
            return str(rawtype)

    args = []
    for param in params.values():
        arg = {"name": param.name}
        if param.annotation != Parameter.empty:
            arg["type"] = parse_type(param.annotation)
        if param.default != Parameter.empty:
            if param.default is None:
                arg["default"] = ""
            else:
                arg["default"] = str(param.default)

        args.append(arg)

    return args
