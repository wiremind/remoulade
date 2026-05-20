from sqlalchemy import text

import remoulade
from remoulade import Message
from remoulade.brokers.pgmq import PgmqBroker

# Just for local test
url = "postgresql://remoulade@localhost:5544/test"
broker = PgmqBroker(url=url, middleware=[])

remoulade.set_broker(broker)
broker.declare_queue("default")

msg = Message(
    queue_name="default",
    actor_name="demo.add",
    args=(1, 2),
    kwargs={},
    options={},
)
broker.enqueue(msg)

with broker.sessionmaker.begin() as session:
    row = session.execute(text("SELECT msg_id, message FROM pgmq.q_default ORDER BY msg_id DESC LIMIT 1")).one()
    print("msg_id:", row.msg_id)
    print("payload:", row.message)

broker.close()
