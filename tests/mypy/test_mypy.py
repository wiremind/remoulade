from mypy import api


def check_mypy_output(fname, expected_output):
    result = api.run([f"tests/mypy/plain_files/{fname}.py"])

    assert [line for line in result[0].split("\n") if "error: " in line or "note: " in line] == expected_output


def test_message():
    check_mypy_output(
        "message",
        [
            'tests/mypy/plain_files/message.py:11: error: Unexpected keyword argument "z" for "message" of "Actor"',
            'tests/mypy/plain_files/message.py:12: error: Too many arguments for "message" of "Actor"',
        ],
    )


def test_result():
    check_mypy_output(
        "result",
        [
            'tests/mypy/plain_files/result.py:9: note: Revealed type is "builtins.int"',
            'tests/mypy/plain_files/result.py:11: note: Revealed type is '
            '"Union[builtins.int, remoulade.results.errors.ErrorStored]"',
        ],
    )


def test_group():
    check_mypy_output(
        "group",
        [
            'tests/mypy/plain_files/group.py:16: note: Revealed type is "typing.Iterator[builtins.str]"',
            'tests/mypy/plain_files/group.py:24: note: Revealed type is '
            '"typing.Iterator[Union[remoulade.results.errors.ErrorStored, builtins.str]]"',
            'tests/mypy/plain_files/group.py:32: note: Revealed type is "typing.Iterator[builtins.object]"',
            'tests/mypy/plain_files/group.py:40: note: Revealed type is "typing.Iterator[Any]"',
        ],
    )

