from remoulade import actor
from typing_extensions import reveal_type


@actor(store_results=True)
def add(x: int, y: int) -> int:
    return x + y


reveal_type(add.message(1, 2))
