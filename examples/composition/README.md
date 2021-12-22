# Remoulade Composition Example
__
This example demonstrates how to use Remoulade's high level composition
abstractions.

## Running the Example

1. Install [Docker][docker].
1. In a terminal window, run `docker-compose up` to run the RabbitMQ and Redis services.
1. Install remoulade and requests: `pip install -r requirements.txt`.
1. In a separate terminal window, run the workers: `remoulade example`.
1. In another terminal, run the example with multiple URLs as argument, for example : `python -m example https://google.com https://rabbitmq.com https://redis.io`.


[docker]: https://docs.docker.com/engine/install/
