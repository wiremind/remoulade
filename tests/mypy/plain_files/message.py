from remoulade import actor


@actor(store_results=True)
def add(x: int, y: int) -> int:
    ...


add.message(1, 2)  # no error
add.message(x=1, y=2)  # no error
add.message(x=1, y=2, z=1)  # mypy error
add.message(1, 2, 3)  # mypy error
