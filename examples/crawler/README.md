# Remoulade Web Crawler Example

This example implements a very simple distributed web crawler using
Remoulade.

## Running the Example

1. Install [Docker][docker].
1. In a terminal, run `docker-compose up`.
1. Install remoulade: `pip install remoulade[rabbitmq]`
1. Install the example's dependencies: `pip install -r requirements.txt`
1. In a separate terminal, run `remoulade example` to run the workers.
1. Finally, run `python example.py https://example.com` to begin
   crawling http://example.com.


[docker]: https://docs.docker.com/engine/install/
