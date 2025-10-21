# Remoulade Persistent Example

This example demonstrates long-running tasks in Remoulade that are durable to
worker shutdowns by using simple state persistence.

## Running the Example

1. Install [Docker][docker].
1. In a terminal, run `docker compose up`.
1. Install remoulade: `pip install remoulade[rabbitmq]`
1. In a separate terminal window, run the workers: `remoulade example`.
1. In another terminal, run `python -m example <n>` to enqueue a task.
1. To test the persistence, terminate the worker process with `crtl-c`, then
   restart with the same `n` value.


[docker]: https://docs.docker.com/engine/install/
