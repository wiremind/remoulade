from remoulade import actor
from typing_extensions import reveal_type


@actor(store_results=True)
def add(x: int, y: int) -> int:
    return x + y


reveal_type(add.send(1, 2).result.get())
reveal_type(add.send(1, 2).result.get(raise_on_error=True))
reveal_type(add.send(1, 2).result.get(raise_on_error=False))
