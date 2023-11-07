import remoulade
from remoulade.middleware import LoggingMetadata


def test_callback(stub_broker):
    # Given that I have two callbacks
    def callback_middleware():
        return {"correlation_id_middleware": "id_middleware", "correlation_id": "id_middleware"}

    def callback_message():
        return {"correlation_id": "id_message"}

    # and a LoggingMetadata Middleware
    stub_broker.add_middleware(LoggingMetadata(logging_metadata_getter=callback_middleware))

    # and a declared actor
    @remoulade.actor(logging_metadata={"correlation_id_actor": "id_actor", "correlation_id": "id_actor"})
    def do_work():
        return 1

    stub_broker.declare_actor(do_work)

    # I build a message with the logging_metadata and logging_metadata_getter options
    message = do_work.message_with_options(
        logging_metadata={"correlation_id": "id_logging_metadata"}, logging_metadata_getter=callback_message
    )

    assert message.options["logging_metadata"] == {
        "correlation_id": "id_message",
        "correlation_id_actor": "id_actor",
        "correlation_id_middleware": "id_middleware",
    }
