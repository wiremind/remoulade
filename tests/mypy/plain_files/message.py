from remoulade import actor


@actor(store_results=True)
def add(x: int, y: int) -> int:
    return x + y


reveal_type(add.message(1, 2))
