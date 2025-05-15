from subprocess import PIPE
import time
from tests.common import remove

fakebroker = object()


def test_cli_fails_to_start_given_an_invalid_broker_name(start_cli):
    # Given that this module doesn't define a broker called "idontexist"
    # When I start the cli and point it at that broker
    proc = start_cli("tests.test_cli:idontexist", stdout=PIPE, stderr=PIPE)
    proc.wait(5)

    # Then the process return code should be 2
    assert proc.returncode == 2


def test_cli_fails_to_start_given_an_invalid_broker_instance(start_cli):
    # Given that this module defines a "fakebroker" variable that's not a Broker
    # When I start the cli and point it at that broker
    proc = start_cli("tests.test_cli:fakebroker", stdout=PIPE, stderr=PIPE)
    proc.wait(5)

    # Then the process return code should be 2
    assert proc.returncode == 2


def run_cli_and_assert(start_cli, args, returncode, filename="test_scrub.pid"):
    try:
        # Pre-create a dummy PID file
        with open(filename, "w") as f:
            f.write("999999")

        proc = start_cli(
            "tests.test_cli_broker",
            extra_cli_args=args,
            extra_args=["--pid-file", filename],
        )

        time.sleep(1)

        proc.terminate()
        proc.wait()

        assert proc.returncode == returncode
    finally:
        remove(filename)


def test_cli_fails_with_invalid_weights_sum(start_cli):
    run_cli_and_assert(start_cli, args=["--queues", "a", "b", "--weights", "a=0.7", "b=0.4"], returncode=1)


def test_cli_fails_with_duplicate_weight_queues(start_cli):
    run_cli_and_assert(start_cli, args=["--queues", "a", "b", "--weights", "a=0.7", "a=0.4"], returncode=1)


def test_cli_fails_with_invalid_weight_types(start_cli):
    run_cli_and_assert(start_cli, args=["--queues", "a", "b", "--weights", "a=0.7z", "a=0.4d"], returncode=1)


def test_cli_succeeds_with_valid_args(start_cli):
    run_cli_and_assert(
        start_cli,
        args=["--queues", "a", "b", "--weights", "a=0.7", "b=0.3", "--prefetch-multiplier", "10"],
        returncode=0,
    )
