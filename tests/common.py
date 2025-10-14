from contextlib import contextmanager

from remoulade import Worker


@contextmanager
def worker(*args, **kwargs):
    try:
        worker = Worker(*args, **kwargs)
        worker.start()
        yield worker
    finally:
        worker.stop()


def get_logs(caplog, msg):
    return [record for record in caplog.records if msg in record.message]
