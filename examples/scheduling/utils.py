"""
This file shows the functions needed to start the worker and scheduler
"""

import subprocess
import sys


def start_worker(broker_module, *, extra_args=None, **kwargs):
    args = [sys.executable, "-m", "remoulade", broker_module]
    proc = subprocess.Popen(args + (extra_args or []), **kwargs)
    return proc


def start_scheduler(broker_module):
    args = ["remoulade-scheduler", broker_module]
    proc = subprocess.Popen(args)
    return proc
