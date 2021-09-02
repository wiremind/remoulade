# Remoulade Basic Example

This example demonstrates how easy it is to get started with Remoulade.

## Running the Example

1. Install [Docker][docker].
1. In a terminal, run `docker run --name basic_rabbitmq -p 5672:5672 rabbitmq`.
1. Install remoulade: `pip install remoulade[rabbitmq]`.
1. In a separate terminal window, run the workers: `remoulade example`.
1. In another terminal, run `python -m example 100` to enqueue 100
   `add` tasks.

[docker]: https://docs.docker.com/engine/install/
