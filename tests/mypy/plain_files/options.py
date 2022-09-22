from remoulade import actor


@actor(store_results=True)
def add(x: int, y: int) -> int:
    ...


add.message_with_options(args=[1, 2], max_retries=10)  # ok
add.message_with_options(args=[1, 2], foo=10)  # mypy error, option `foo` is unknown
