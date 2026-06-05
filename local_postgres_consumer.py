import time

import remoulade
from remoulade import Worker
from remoulade.brokers.postgres import PostgresBroker

# Same DSN as local_postgres_broker.py
URL = "postgresql://remoulade@localhost:5544/test"
QUEUE_NAME = "default"
ACTOR_NAME = "demo.add"

broker = PostgresBroker(
    url=URL,
    middleware=[],
)
remoulade.set_broker(broker)
broker.declare_queue(QUEUE_NAME)


@remoulade.actor(actor_name=ACTOR_NAME, queue_name=QUEUE_NAME)
def add(x: int, y: int) -> int:
    result = x + y
    print(f"[demo.add] {x} + {y} = {result}")
    return result


remoulade.declare_actors([add])


if __name__ == "__main__":
    probe = broker.consume(QUEUE_NAME, prefetch=1, timeout=30_000)
    listener_mode = "LISTEN/NOTIFY" if probe._listener_available else "polling fallback"
    probe.close()

    worker = Worker(broker, queues={QUEUE_NAME}, worker_threads=1, worker_timeout=30_000)
    worker.start()
    print(f"Postgres local consumer started on queue '{QUEUE_NAME}' ({listener_mode}). Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        worker.stop()
        broker.close()
