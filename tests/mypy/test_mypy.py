import pathlib

from mypy import api


def check_mypy_output(fname, expected_output):
    mypy_dir = pathlib.Path(__file__).parent.resolve()
    result = api.run([str(mypy_dir / "plain_files" / f"{fname}.py"), "--config-file", str(mypy_dir / "mypy.ini")])

    assert [line for line in result[0].split("\n") if "error: " in line or "note: " in line] == expected_output, str(
        result
    )


def test_actor():
    check_mypy_output(
        "actor",
        [
            'tests/mypy/plain_files/actor.py:14: note: Revealed type is "remoulade.actor.Actor[[x: builtins.int, y: builtins.int =], builtins.int]"',
            'tests/mypy/plain_files/actor.py:15: note: Revealed type is "remoulade.actor.Actor[[x: builtins.int, y: builtins.int], builtins.float]"',
            'tests/mypy/plain_files/actor.py:19: note: Revealed type is "builtins.int"',
            'tests/mypy/plain_files/actor.py:25: error: Argument 1 to "__call__" of "Actor" has incompatible type "str"; expected "int"  [arg-type]',
        ],
    )


def test_message():
    check_mypy_output(
        "message",
        [
            'tests/mypy/plain_files/message.py:9: note: Revealed type is "remoulade.message.Message[remoulade.result.Result[builtins.int]]"'
        ],
    )


def test_result():
    check_mypy_output(
        "result",
        [
            'tests/mypy/plain_files/result.py:9: note: Revealed type is "builtins.int"',
            'tests/mypy/plain_files/result.py:10: note: Revealed type is "builtins.int"',
            'tests/mypy/plain_files/result.py:11: note: Revealed type is "builtins.int | remoulade.results.errors.ErrorStored"',
        ],
    )


def test_group():
    check_mypy_output(
        "composition",
        [
            'tests/mypy/plain_files/composition.py:22: note: Revealed type is "typing.Generator[builtins.int, None, None]"',
            'tests/mypy/plain_files/composition.py:24: note: Revealed type is "typing.Generator[builtins.int | None, None, None]"',
            'tests/mypy/plain_files/composition.py:27: note: Revealed type is "builtins.int"',
            'tests/mypy/plain_files/composition.py:32: note: Revealed type is "remoulade.result.Result[None]"',
            'tests/mypy/plain_files/composition.py:41: note: Revealed type is "typing.Generator[builtins.int, None, None]"',
            'tests/mypy/plain_files/composition.py:45: note: Revealed type is "typing.Generator[Any, None, None]"',
        ],
    )
