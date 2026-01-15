# Remoulade Results Example

This example demonstrates how to use Remoulade actors to store and
retrieve task results.

## Running the Example

1. Install [Docker][docker].
1. In a terminal, run `docker-compose up`.
1. Install remoulade: `pip install remoulade[rabbitmq]`
1. In a separate terminal window, run the workers: `remoulade example`.
1. In another terminal, run `python -m example`.
1. For PostgreSQL, install `pip install remoulade[postgres]` and run `python -m postgres_example`.


[docker]: https://docs.docker.com/engine/install/
