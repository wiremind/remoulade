import remoulade
from remoulade import pipeline


class TestPipeLines:

    """ Class to test PipeLine Logic """

    def test_create_pipeline(self, stub_broker, stub_worker):
        @remoulade.actor
        def add(x, y=0):
            return x + y
        stub_broker.declare_actor(add)
        pipe = pipeline((add.message(i) for i in range(4)))
        print(dir(pipe), pipe.pipeline_id)
