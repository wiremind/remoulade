from ..errors import RemouladeError


class StateError(RemouladeError):
    """Base class for State Errors"""


class InvalidStateError(StateError):
    """Raised when you try to declare an state
    that is not defined.
    """
