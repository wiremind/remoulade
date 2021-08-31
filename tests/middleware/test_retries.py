import remoulade
from remoulade import Middleware
from remoulade.middleware import Retries


def test_retry_message_is_copy(stub_broker, stub_worker):
    retries_original_message, retries_retried_message = None, None

    class TestMiddleware(Middleware):

        default_before = Retries

        def after_process_message(self, broker, message, *, result=None, exception=None):
            nonlocal retries_original_message, retries_retried_message
            if retries_original_message is None:
                retries_original_message = message.options.get('retries') or 0
            else:
                retries_retried_message = message.options["retries"]

    stub_broker.add_middleware(TestMiddleware())

    @remoulade.actor(max_retries=1, min_backoff=1)
    def do_work():
        raise Exception()

    stub_broker.declare_actor(do_work)

    do_work.send()
    stub_broker.join(do_work.queue_name)
    stub_worker.join()

    assert retries_original_message == 0
    assert retries_retried_message == 1
