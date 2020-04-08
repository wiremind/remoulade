import argparse
import importlib
import logging
import signal
import sys

from remoulade import get_logger, get_scheduler

logformat = "[%(asctime)s] [PID %(process)d] [%(threadName)s] [%(name)s] [%(levelname)s] %(message)s"


def parse_arguments():
    parser = argparse.ArgumentParser(
        prog="remoulade-scheduler",
        description="Run remoulade scheduler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "modules", metavar="module", nargs="*", help="additional python modules to import",
    )
    parser.add_argument("--path", "-P", default=".", nargs="*", type=str, help="the module import path (default: .)")
    parser.add_argument("--verbose", "-v", action="count", default=0, help="turn on verbose log output")
    return parser.parse_args()


def main():

    args = parse_arguments()

    for path in args.path:
        sys.path.insert(0, path)

    for module in args.modules:
        importlib.import_module(module)

    logging.basicConfig(level=logging.INFO if not args.verbose else logging.DEBUG, format=logformat)
    logger = get_logger("remoulade", "Scheduler")

    def signal_handler(signal, frame):
        logger.debug("Remoulade scheduler is shutting down")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.debug("Remoulade scheduler start")
    sys.exit(get_scheduler().start())
