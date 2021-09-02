import remoulade
from remoulade.worker import build_extra

from .common import get_logs, worker


def test_workers_dont_register_queues_that_arent_whitelisted(stub_broker):
    # Given that I have a worker object with a restricted set of queues
    with worker(stub_broker, queues={"a", "b"}) as stub_worker:
        # When I try to register a consumer for a queue that hasn't been whitelisted
        stub_broker.declare_queue("c")
        stub_broker.declare_queue("c.DQ")

        # Then a consumer should not get spun up for that queue
        assert "c" not in stub_worker.consumers
        assert "c.DQ" not in stub_worker.consumers


def test_build_extra(stub_broker):
    # Given that I have an actor
    @remoulade.actor
    def do_work():
        return 1

    # and the actor is declared
    stub_broker.declare_actor(do_work)

    # I build a message
    message = do_work.message()

    # I build the extra
    extra = build_extra(message)

    # I expect the extra to look like this
    assert extra == {"message_id": message.message_id, "input": {"args": "()", "kwargs": "{}"}}


def test_build_extra_metadata(stub_broker):
    # Given that I have an actor
    @remoulade.actor
    def do_work():
        return 1

    # and the actor is declared
    stub_broker.declare_actor(do_work)

    # I build a message with logging_metadata
    message = do_work.message_with_options(logging_metadata={"id": "1"})

    # I build the extra
    extra = build_extra(message)

    # I expect the find the logging_metadata in the extra
    assert extra == {"id": "1", "message_id": message.message_id, "input": {"args": "()", "kwargs": "{}"}}


def test_build_extra_override(stub_broker):
    # Given that I have an actor
    @remoulade.actor
    def do_work():
        return 1

    # and the actor is declared
    stub_broker.declare_actor(do_work)

    # I build a message with logging_metadata that has a key already used in the extra
    message = do_work.message_with_options(logging_metadata={"message_id": "1"})

    # I build the extra
    extra = build_extra(message)

    # I expect the 'message_id' value to be the message.message_id
    assert extra == {"message_id": message.message_id, "input": {"args": "()", "kwargs": "{}"}}


def test_logging_metadata_logs(stub_broker, stub_worker, caplog):
    # Given that I have an actor
    @remoulade.actor
    def do_work():
        return 1

    # and the actor is declared
    stub_broker.declare_actor(do_work)

    # I build a message with logging_metadata
    do_work.send_with_options(logging_metadata={"id": "1"})

    # And join on the queue
    stub_broker.join(do_work.queue_name)
    stub_worker.join()

    # I expect to find the metadata in the logs extra
    records = get_logs(caplog, "Started Actor")
    assert len(records) == 1
    assert records[0].levelname == "INFO"
    assert records[0].__dict__["id"] == "1"
