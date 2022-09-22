from remoulade import actor


@actor(store_results=True)
def add(x: int, y: int) -> int:
    ...


reveal_type(add.send(1, 2).result.get())  # builtins.int
reveal_type(
    add.send(1, 2).result.get(raise_on_error=False)
)  #  Union[builtins.int, remoulade.results.errors.ErrorStored]
