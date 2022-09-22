from remoulade import actor
from remoulade.composition import group, pipeline


@actor(store_results=True)
def add(x: int, y: int) -> int:
    ...


@actor(store_results=True)
def request(uri: str) -> str:
    ...


reveal_type(
    group(
        [
            request.message_with_options(args=["y"]),
            request.message_with_options(args=["x"]),
        ]
    ).results.get()
)  # typing.Iterator[builtins.str]
reveal_type(
    group(
        [
            request.message_with_options(args=["y"]),
            request.message_with_options(args=["x"]),
        ]
    ).results.get(raise_on_error=False)
)  # typing.Iterator[Union[remoulade.results.errors.ErrorStored, builtins.str]]
reveal_type(
    group(
        [
            request.message_with_options(args=["y"]),
            add.message(1, 2),
            request.message_with_options(args=["x"]),
        ]
    ).results.get()
)  # typing.Iterator[builtins.object]
reveal_type(group([pipeline([])]).results.get())  # typing.Iterator[A]
