import subprocess
import sys
from itertools import product

import pytest

from remoulade.helpers.pgmq_names import (
    QUEUE_NAME_MAX_LENGTH,
    assert_valid_pgmq_queue_name,
    assert_valid_queue_name,
    logical_queue_name,
    pgmq_table_name,
    physical_queue_name,
)

VALID_QUEUE_NAMES = [
    "default",
    "sales",
    "sales_eu",
    "sales-eu",
    "_x",
    "etl.low",
    "clickhouse.stream.high",
    "read.medium",
    "a..b",
    "trailing.",
    "a_b.c-d",
    "q" * QUEUE_NAME_MAX_LENGTH,
    "a." * 15 + "ab",
]


def _is_valid(queue_name):
    try:
        assert_valid_queue_name(queue_name)
    except ValueError:
        return False
    return True


def _is_valid_pgmq(pgmq_queue_name):
    try:
        assert_valid_pgmq_queue_name(pgmq_queue_name)
    except ValueError:
        return False
    return True


@pytest.mark.parametrize(
    ("queue_name", "pgmq_queue_name"),
    [
        ("etl.low", "etl__low"),
        ("clickhouse.stream.high", "clickhouse__stream__high"),
        ("a..b", "a____b"),
        ("trailing.", "trailing__"),
        ("default", "default"),
        ("sales_eu", "sales_eu"),
        ("sales-eu", "sales-eu"),
        ("_x", "_x"),
    ],
)
def test_physical_queue_name_replaces_each_dot_with_a_double_underscore(queue_name, pgmq_queue_name):
    assert physical_queue_name(queue_name) == pgmq_queue_name
    assert logical_queue_name(pgmq_queue_name) == queue_name


@pytest.mark.parametrize("queue_name", VALID_QUEUE_NAMES)
def test_logical_queue_name_inverts_physical_queue_name(queue_name):
    assert_valid_queue_name(queue_name)
    pgmq_queue_name = physical_queue_name(queue_name)

    assert "." not in pgmq_queue_name
    assert logical_queue_name(pgmq_queue_name) == queue_name
    assert physical_queue_name(pgmq_queue_name) == pgmq_queue_name
    assert_valid_pgmq_queue_name(pgmq_queue_name)


@pytest.mark.parametrize("queue_name", ["default", "sales_eu", "sales-eu", "_x", "q" * QUEUE_NAME_MAX_LENGTH])
def test_physical_queue_name_keeps_dot_free_names(queue_name):
    assert physical_queue_name(queue_name) == queue_name


def test_pgmq_table_name_names_the_queue_and_archive_tables():
    assert pgmq_table_name("etl.low") == "q_etl__low"
    assert pgmq_table_name("etl.low", "a") == "a_etl__low"
    assert pgmq_table_name("etl__low") == "q_etl__low"
    assert pgmq_table_name("default", "a") == "a_default"


def test_the_mapping_is_injective_and_onto_the_valid_pgmq_names():
    alphabet = "a_.-"
    names = ["".join(chars) for length in range(1, 9) for chars in product(alphabet, repeat=length)]
    valid = [name for name in names if _is_valid(name)]
    physical_to_logical = {}
    for name in valid:
        pgmq_queue_name = physical_queue_name(name)
        assert physical_to_logical.setdefault(pgmq_queue_name, name) == name
        assert logical_queue_name(pgmq_queue_name) == name

    valid_pgmq_names = {name for name in names if "." not in name and _is_valid_pgmq(name)}
    assert valid_pgmq_names == {name for name in physical_to_logical if len(name) <= 8}
    assert len(valid) > 1000


@pytest.mark.parametrize(
    "queue_name",
    [
        "",
        "q" * (QUEUE_NAME_MAX_LENGTH + 1),
        "a." * 16 + "a",
        'default" ; DROP TABLE pgmq.q_default; --',
        "default'",
        "with space",
        "with\nnewline",
        "accentué",
        "semi;colon",
        "back\\slash",
        "1leading-digit",
        ".leading-dot",
        "etl__low",
        "etl_.low",
        "etl._low",
        "etl___low",
        "Default",
        "etl.Low",
        "sales.DQ",
        "sales.XQ",
    ],
)
def test_assert_valid_queue_name_rejects(queue_name):
    with pytest.raises(ValueError, match="not a usable queue name"):
        assert_valid_queue_name(queue_name)


def test_the_length_limit_applies_to_the_pgmq_name():
    queue_name = "a." * 16 + "a"

    assert len(queue_name) <= QUEUE_NAME_MAX_LENGTH
    with pytest.raises(ValueError, match=f"at most {QUEUE_NAME_MAX_LENGTH} characters"):
        assert_valid_queue_name(queue_name)


@pytest.mark.parametrize(
    ("queue_name", "reason"),
    [
        ("etl__low", "must not contain"),
        ("sales.DQ", "uppercase"),
        ("1leading-digit", "SQL identifier"),
    ],
)
def test_assert_valid_queue_name_says_why(queue_name, reason):
    with pytest.raises(ValueError, match=reason):
        assert_valid_queue_name(queue_name)


@pytest.mark.parametrize("pgmq_queue_name", ["default", "etl__low", "a____b", "q" * QUEUE_NAME_MAX_LENGTH])
def test_assert_valid_pgmq_queue_name_accepts(pgmq_queue_name):
    assert assert_valid_pgmq_queue_name(pgmq_queue_name) is None


@pytest.mark.parametrize(
    "pgmq_queue_name",
    [
        "",
        "etl.low",
        "etl___low",
        "Etl__low",
        "q" * (QUEUE_NAME_MAX_LENGTH + 1),
        'default" ; DROP TABLE pgmq.q_default; --',
        "with space",
    ],
)
def test_assert_valid_pgmq_queue_name_rejects(pgmq_queue_name):
    with pytest.raises(ValueError, match="not a usable queue name"):
        assert_valid_pgmq_queue_name(pgmq_queue_name)


def test_pgmq_names_imports_without_the_postgres_extra():
    code = (
        "import sys\n"
        "sys.modules['pgmq'] = None\n"
        "sys.modules['psycopg'] = None\n"
        "sys.modules['sqlalchemy'] = None\n"
        "from remoulade.helpers.pgmq_names import physical_queue_name\n"
        "print(physical_queue_name('etl.low'))\n"
    )
    completed = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)

    assert completed.stdout.strip() == "etl__low"
