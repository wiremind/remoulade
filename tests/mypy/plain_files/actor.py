from remoulade import actor


@actor(store_results=True)
def add(x: int, y: int = 0) -> int:
    return x + y


@actor
def div(x: int, y: int) -> float:
    return x / y


reveal_type(add)
reveal_type(div)

# OK
y = add(x=1, y=2)
reveal_type(y)
add(1, y=2)
add(1, 2)
add(1)

# NOK
add("s")
