# Remoulade Long Running Example

This example demonstrates extremely long-running tasks in Remoulade.

## Running the Example

1. Install [Docker][docker].
1. In a terminal, run `docker compose up`.
1. Install remoulade: `pip install remoulade[rabbitmq]`
1. In a separate terminal window, run the workers: `remoulade example`.
1. In another terminal, run `python -m example` to enqueue a task.


[docker]: https://docs.docker.com/engine/install/

