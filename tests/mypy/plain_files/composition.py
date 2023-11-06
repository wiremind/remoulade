from typing import Any
from typing_extensions import reveal_type

from remoulade import actor, group, pipeline


@actor(store_results=True)
def add(x: int, y: int) -> int:
    return x + y


@actor(store_results=True)
def _print(x: str) -> None:
    print(x)


@actor(store_results=True)
def stop(x: Any) -> None:
    return


# simple homogeneous group # both pyright and mypy can infer  that this will yield integers
reveal_type(group([add.message(1, x) for x in range(10)]).results.get())
# heterogeneous group # both pyright and mypy can infer  that this will yield integeror None items
reveal_type(group([add.message(1, 2), _print.message("hello")]).results.get())

# simple pipeline # both pyright and mypy can infer  that this will return an int
reveal_type(pipeline((add.message(1, 2), add.message(1))).result.get())
# pipeline with a bunch of different actors
# pyright can infer  that this will return Result[None]
# mypy in its development version can do the same
# but current stable version infers this as <nothing>
reveal_type(pipeline((_print.message("a"), _print.message(), stop.message())).result)
# pipeline that ends with a group of actors
# pyright can infer this yields integers, mypy infers this yields Any. It is not as good, but we still get the fact this is a generator, which is nice.
pipe = pipeline(
    (
        add.message(1, 2),
        group([add.message(1, 2), add.message(1, 2)]),
    )
)
reveal_type(pipe.result.get())

# group with pipeline that ends with a group
# this is starting to be a very complex object, both pyright and mypy infer this as Any generator.
reveal_type(group([pipe]).results.get())
