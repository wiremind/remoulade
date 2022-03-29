from functools import update_wrapper

from apispec import APISpec
from apispec.ext.marshmallow import MarshmallowPlugin
from flask import request
from flask_apispec import FlaskApiSpec, use_kwargs


def validate_schema(schema):
    def decorator(func):
        def wrapper(*args, **kwargs):
            if request.is_json and request.content_length and request.content_length > 0:
                body = request.get_json()
            else:
                body = {}
            res = schema().load(body)
            return func(*args, **res, **kwargs)

        return use_kwargs(schema, apply=False)(update_wrapper(wrapper, func))

    return decorator


def add_swagger(app, routes_dict):
    app.config.update(
        {
            "APISPEC_SPEC": APISpec(
                title="Remoulade API", version="v1", plugins=[MarshmallowPlugin()], openapi_version="3.0.3"
            )
        }
    )
    api_spec = FlaskApiSpec(document_options=False)

    for name, route_list in routes_dict.items():
        for route in route_list:
            api_spec.register(route, blueprint=name if name != "main" else None)
    api_spec.init_app(app)
