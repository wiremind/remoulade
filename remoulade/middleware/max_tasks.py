import threading

from remoulade import Middleware, get_logger


class MaxTasks(Middleware):
    """Middleware that stop a worker if its amount of tasks completed
    If a task causes a worker to exceed this limit, the task will be completed, and the worker will be
    stopped afterwards.

    Parameters:
      max_tasks(int): The maximum amount of resident memory (in kilobytes)
    """

    def __init__(self, *, max_tasks: int):
        self.logger = get_logger(__name__, type(self))
        self.max_tasks = max_tasks
        self.tasks_count = 0
        self.lock = threading.Lock()

    def after_worker_thread_process_message(self, broker, thread):
        with self.lock:
            self.tasks_count += 1
            tasks_count = self.tasks_count
            max_tasks = self.max_tasks
        if tasks_count == max_tasks:
            self.logger.warning(
                f"Stopping worker thread as tasks completed ({tasks_count}) >= max_tasks ({tasks_count})"
            )
            # stopping the worker thread will in time stop the worker process which check if all workers are still
            # running (via Worker.worker_stopped)
            thread.stop()