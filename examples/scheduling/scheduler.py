"""
    Example to schedule a task to run in a given interval
"""

import time

from utils import start_scheduler, start_worker


def main():
    task_name = "task_scheduler"

    # start scheduler and worker
    scheduler = start_scheduler(task_name)
    worker = start_worker(task_name, extra_args=["--processes", "1", "--threads", "1"])

    # stop the scheduler after one minute
    time.sleep(60)
    scheduler.terminate()
    worker.terminate()


if __name__ == "__main__":
    main()
