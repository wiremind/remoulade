import argparse
import importlib
import sys

from remoulade import get_broker


def parse_arguments():
    parser = argparse.ArgumentParser(
        prog="remoulade-ls",
        description="List of remoulade actors",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "modules", metavar="module", nargs="*", help="additional python modules to import",
    )
    parser.add_argument("--path", "-P", default=".", nargs="*", type=str, help="the module import path (default: .)")
    return parser.parse_args()


def main():

    args = parse_arguments()

    for path in args.path:
        sys.path.insert(0, path)

    for module in args.modules:
        importlib.import_module(module)

    print("List of available actors:")
    for actor in get_broker().actors:
        print(actor)
