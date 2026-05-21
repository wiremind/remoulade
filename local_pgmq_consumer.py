import time

import remoulade
from remoulade import Worker
from remoulade.brokers.pgmq import PgmqBroker

# Same DSN as local_pgmq_broker.py
URL = "postgresql://remoulade@localhost:5544/test"

broker = PgmqBroker(url=URL, middleware=[])
remoulade.set_broker(broker)


@remoulade.actor(actor_name="demo.add", queue_name="default")
def add(x: int, y: int) -> int:
    result = x + y
    print(f"[demo.add] {x} + {y} = {result}")
    return result


remoulade.declare_actors([add])


if __name__ == "__main__":
    worker = Worker(broker, queues={"default"}, worker_threads=1, worker_timeout=500)
    worker.start()
    print("PGMQ local consumer started on queue 'default'. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        worker.stop()
        broker.close()
