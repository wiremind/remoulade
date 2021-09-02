import threading

from ..logging import get_logger
from .middleware import Middleware


class MaxTasks(Middleware):
    """Middleware that stop a worker if its amount of tasks completed reach max_tasks.
    If a task causes a worker to reach this limit, all other worker threads will complete their task, and the worker
    will be stopped afterwards.

    Parameters:
      max_tasks(int): The amount of tasks to complete before stopping the worker
    """

    def __init__(self, *, max_tasks: int):
        self.logger = get_logger(__name__, type(self))
        self.max_tasks = max_tasks
        self.tasks_count = 0
        self.lock = threading.Lock()

    def after_worker_thread_process_message(self, broker, thread):
        with self.lock:
            self.tasks_count += 1
            if self.tasks_count == self.max_tasks:
                self.logger.info(
                    f"Stopping worker thread as completed tasks ({self.tasks_count}) > max_tasks ({self.max_tasks})"
                )
                thread.stop()
