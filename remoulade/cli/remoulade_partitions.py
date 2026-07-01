"""``remoulade-partitions`` CLI.

One-shot maintenance command for the PostgreSQL broker. It creates a minimal
``PostgresBroker`` from the ``REMOULADE_POSTGRES_URL`` environment variable (no
middleware, no LISTEN/NOTIFY connection), enables ``infinite_time_partitions``
on every PGMQ queue and archive partition set so ``pg_partman`` keeps
maintaining partitions up to the current time even after a gap in activity, and
then runs ``pg_partman`` maintenance to apply it immediately.
"""

import argparse
import os

from remoulade.brokers.postgres import PostgresBroker

POSTGRES_URL_ENV_VAR = "REMOULADE_POSTGRES_URL"


def parse_arguments():
    parser = argparse.ArgumentParser(
        prog="remoulade-partitions",
        description=(
            "Enable infinite_time_partitions on every PostgreSQL queue and archive partition set, "
            f"then run pg_partman maintenance. The database URL is read from ${POSTGRES_URL_ENV_VAR}."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    return parser.parse_args()


def main():
    """Enable infinite_time_partitions on all queue/archive partition sets, then run maintenance."""
    parse_arguments()

    url = os.getenv(POSTGRES_URL_ENV_VAR)
    if not url:
        print(f"{POSTGRES_URL_ENV_VAR} environment variable is not set")
        return 1

    broker = PostgresBroker(url=url, middleware=[], enable_listen_notify=False)
    try:
        tables = broker.enable_infinite_time_partitions()
        print(f"Enabled infinite_time_partitions on {len(tables)} partition sets:")
        for table in tables:
            print(f"  {table}")

        broker.run_partition_maintenance()
        print("Ran pg_partman maintenance.")
    finally:
        broker.close()

    return 0
