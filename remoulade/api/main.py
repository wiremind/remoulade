"""This file describe the API to get the state of messages"""

from flask import Flask
from marshmallow import ValidationError
from werkzeug.exceptions import HTTPException

from remoulade.errors import RemouladeError

from .apispec import add_swagger

app = Flask(__name__)


@app.errorhandler(RemouladeError)
def remoulade_exception(e):
    return {"error": str(e)}, 500


@app.errorhandler(HTTPException)
def http_exception(e):
    return {"error": str(e)}, e.code


@app.errorhandler(ValidationError)
def validation_error(e):
    return {"error": e.normalized_messages()}, 400


add_swagger(app, {"main": []})
