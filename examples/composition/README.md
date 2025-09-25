# Remoulade Composition Example

__
This example demonstrates how to use Remoulade's high level composition
abstractions.

## Running the Example

- Install [Docker](https://docs.docker.com/engine/install/).
- In a terminal window, run `docker compose -f compose/compose.yaml up` to run the RabbitMQ and Redis services.
- Install this example, ideally in a virtual env: `cd examples/composition && pip install -e .`.
- In a separate terminal window, run the worker (defined as entry_point): `composition_worker`.
- In another terminal, run the example with multiple URLs as argument, for example : `composition_ask_count_words https://google.com https://rabbitmq.com https://redis.io`.
