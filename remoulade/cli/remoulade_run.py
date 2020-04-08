import argparse
import importlib
import json
import sys

from remoulade import get_broker


def parse_arguments():
    parser = argparse.ArgumentParser(
        prog="remoulade-run",
        description="Runs a remoulade actor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "modules", metavar="module", nargs="*", help="additional python modules to import",
    )
    parser.add_argument("--path", "-P", default=".", nargs="*", type=str, help="the module import path (default: .)")
    parser.add_argument("--actor-name", "-N", type=str, help="The actor to be ran")
    parser.add_argument("--args", "-A", type=json.loads, help="The actor's args")
    parser.add_argument("--kwargs", "-K", type=json.loads, help="The actor's kwargs")
    return parser.parse_args()


def main():

    args = parse_arguments()

    for path in args.path:
        sys.path.insert(0, path)

    for module in args.modules:
        importlib.import_module(module)

    if args.actor_name not in get_broker().actors:
        print("{} is not an available actor".format(args.actor_name))
        return

    _args = args.args or []
    kwargs = args.kwargs or {}

    print(get_broker().actors[args.actor_name](*_args, **kwargs))
