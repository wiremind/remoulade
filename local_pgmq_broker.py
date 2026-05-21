from collections.abc import Iterable
from typing import Any, cast

from sqlalchemy import text

import remoulade
from remoulade import Message
from remoulade.brokers.pgmq import PgmqBroker

# Just for local test
URL = "postgresql://remoulade@localhost:5544/test"
QUEUE_NAME = "default"
ACTOR_NAME = "demo.add"

broker = PgmqBroker(
    url=URL,
    middleware=[],
    listen_notify_enabled=True,
)
remoulade.set_broker(broker)
broker.declare_queue(QUEUE_NAME)

msg_args: Iterable[Any] = cast(Iterable[Any], (1, 2))
msg: Message = Message(
    queue_name=QUEUE_NAME,
    actor_name=ACTOR_NAME,
    args=msg_args,
    kwargs={},
    options={},
)
broker.enqueue(msg)

with broker.sessionmaker.begin() as session:
    row = session.execute(text("SELECT msg_id, message FROM pgmq.q_default ORDER BY msg_id DESC LIMIT 1")).one()
    print("msg_id:", row.msg_id)
    print("payload:", row.message)
    print("listen_notify_channel:", f"pgmq.q_{QUEUE_NAME}.INSERT")

broker.close()
