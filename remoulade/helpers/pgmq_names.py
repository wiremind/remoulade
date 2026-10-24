from typing import Final, Literal

from ..actor import QUEUE_NAME_PATTERN

QUEUE_NAME_MAX_LENGTH: Final[int] = 47
PHYSICAL_DOT: Final[str] = "__"
AMBIGUOUS_SUBSTRINGS: Final[tuple[str, ...]] = (PHYSICAL_DOT, "_.", "._")


def physical_queue_name(queue_name: str) -> str:
    return queue_name.replace(".", PHYSICAL_DOT)


def logical_queue_name(pgmq_queue_name: str) -> str:
    return pgmq_queue_name.replace(PHYSICAL_DOT, ".")


def pgmq_table_name(queue_name: str, prefix: Literal["q", "a"] = "q") -> str:
    return f"{prefix}_{physical_queue_name(queue_name)}"


def _queue_name_problem(queue_name: str) -> str | None:
    if not QUEUE_NAME_PATTERN.fullmatch(queue_name):
        return (
            "it becomes a SQL identifier, so it must start with a letter or an underscore and hold only letters, "
            "digits, dashes, dots and underscores"
        )
    if queue_name != queue_name.lower():
        return "pgmq lowercases the tables it creates for a queue, so it must not hold any uppercase letter"
    if any(substring in queue_name for substring in AMBIGUOUS_SUBSTRINGS):
        return (
            f"each dot becomes {PHYSICAL_DOT!r} in pgmq, so it must not contain "
            f"{', '.join(repr(substring) for substring in AMBIGUOUS_SUBSTRINGS)}"
        )
    if len(physical_queue_name(queue_name)) > QUEUE_NAME_MAX_LENGTH:
        return (
            f"it must be at most {QUEUE_NAME_MAX_LENGTH} characters long once each dot becomes {PHYSICAL_DOT!r} "
            f"(pgmq name {physical_queue_name(queue_name)!r})"
        )
    return None


def assert_valid_queue_name(queue_name: str) -> None:
    problem = _queue_name_problem(queue_name)
    if problem is not None:
        raise ValueError(f"{queue_name!r} is not a usable queue name for a PostgresBroker: {problem}.")


def assert_valid_pgmq_queue_name(pgmq_queue_name: str) -> None:
    queue_name = logical_queue_name(pgmq_queue_name)
    problem = _queue_name_problem(queue_name)
    if problem is None and physical_queue_name(queue_name) != pgmq_queue_name:
        problem = f"a pgmq name never holds a dot, since each dot of a queue name becomes {PHYSICAL_DOT!r}"
    if problem is not None:
        raise ValueError(
            f"{pgmq_queue_name!r} is not a usable queue name for pgmq (queue name {queue_name!r}): {problem}."
        )
