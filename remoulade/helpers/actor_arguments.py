from inspect import Parameter, signature


def get_actor_arguments(actor):
    params = signature(actor.fn).parameters

    args = []
    for param in params.values():
        arg = {"name": param.name}
        if param.annotation != Parameter.empty:
            arg["type"] = str(param.annotation)
        if param.default != Parameter.empty:
            if param.default is None:
                arg["default"] = ""
            else:
                arg["default"] = str(param.default)

        args.append(arg)

    return args
