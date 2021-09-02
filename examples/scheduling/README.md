# Remoulade Scheduling Example

This example demonstrates how to use the scheduler of remoulade.

### Jobs
In this example, the scheduler will be used to run the following tasks :
- Count the words of https://github.com every second
- Count the words of https://gitlab.com every ten seconds

### Instructions

1. Install [Docker][docker].
1. In a terminal, run `docker-compose up`.
1. Install remoulade: `pip install remoulade[rabbitmq]`
1. In a separate terminal window, run the workers: `remoulade example`.
1. In another terminal, run `python -m example`.


[docker]: https://docs.docker.com/engine/install/
