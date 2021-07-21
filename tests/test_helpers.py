import remoulade
from remoulade.helpers import reduce, get_actor_arguments
from remoulade.results import Results


def test_reduce_messages(stub_broker, stub_worker, result_backend):
    # Given a result backend
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=result_backend))

    # Given an actor that stores results
    @remoulade.actor(store_results=True)
    def do_work():
        return 1

    # Given an actor that stores results
    @remoulade.actor(store_results=True)
    def merge(results):
        return sum(results)

    # And this actor is declared
    stub_broker.declare_actor(do_work)
    stub_broker.declare_actor(merge)

    merged_message = reduce((do_work.message() for _ in range(10)), merge)
    merged_message.run()

    result = merged_message.result.get(block=True)

    assert 10 == result


def test_actor_arguments():
    @remoulade.actor
    def do_work(a: int = None):
        return 1

    assert get_actor_arguments(do_work) == [{"default": "", "name": "a", "type": "int"}]
