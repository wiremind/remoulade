import remoulade
from remoulade import pipeline


class TestPipeLines:

    """ Class to test PipeLine Logic """

    def test_create_pipeline(self, stub_broker, stub_worker, state_middleware):
        @remoulade.actor
        def add(x, y=0):
            return x + y

        stub_broker.declare_actor(add)
        messages = [add.message(1), add.message(2), add.message(42)]
        pipe = pipeline(messages).run()
        assert pipe.pipeline_id is not None
