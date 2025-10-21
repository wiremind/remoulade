.. include:: global.rst

Getting Started
===============

This part of the docs will walk you through the main features of remoulade by building a simple feature that will fetch temperatures from different cities using weather-api_ and store them.

By the end of this tutorial, you will be able to do the following:

* :ref:`create a remoulade worker and send it tasks asynchronously<getting_started:Adding an Actor>`
* :ref:`create a group of tasks and execute them in parallel<getting_started:Using a Group of tasks>`
* :ref:`create a pipeline of tasks that will sequentially process data<getting_started:Creating a Pipeline of tasks>`
* :ref:`use the result middleware to wait and get actor results<getting_started:Using the Result Middleware>`
* :ref:`use the remoulade scheduler to periodically run tasks<getting_started:Scheduling Messages>`
* :ref:`use SuperBowl to monitor and manage your tasks<getting_started:Monitoring and Managing your tasks>`

Prerequisites
-------------

To follow this guide, you will need Python_ >= 3.12.

You will also need RabbitMQ_ as a broker. If you don't have it installed, you can use Docker_ to run a RabbitMQ image::

  $ docker run -d --name rabbitmq -p 5672:5672 rabbitmq


To follow this tutorial, it is recommended to create a new directory with a virtual environment::

   $ mkdir remoulade-tutorial
   $ cd remoulade-tutorial
   $ python3 -m venv env
   $ source env/bin/activate
   $ python --version

.. _Python: https://www.python.org/downloads/

Project Setup
-------------

This guide will use Requests_ and Remoulade with RabbitMQ_::

  $ pip install -U 'remoulade[rabbitmq]' requests

.. _requests: http://docs.python-requests.org
.. _Docker: https://docs.docker.com/engine/install/

Adding an Actor
---------------
In this guide, we will build a simple feature that will fetch the weather forecasts from different cities using weather-api_ and store them. 
For simplicity purposes, we will visualize our gathered data through endpoints.dev_ rather than a database.

.. _weather-api: https://github.com/robertoduessmann/weather-api
.. _endpoints.dev: https://www.endpoints.dev/

In order to be able to see the gathered data, go to endpoints.dev_ and click on the url at the top to copy your endpoint url. It will be helpful for the function we are about to write.

Let's start by writing a simple function that fetches the temperature for a single city:

.. code-block:: python
   :caption: get_weather.py

    import requests

    url_endpoint = "https://www.<unique-id>.endpoints.dev/"  # put your unique endpoint here

    def get_weather(city):
        url = f"https://goweather.herokuapp.com/weather/{city}"

        response = requests.get(url).json()

        text = f'{city}: {response["description"]}'
        requests.post(url_endpoint, json=text)

This script simply gets the weather for a city and makes a POST request to endpoints.dev_ with the city and the weather description in the request body.
We can now run this function in a python shell::

   $ python
   >>> from get_weather import get_weather
   >>> get_weather("Paris")

After running this function, you will see at endpoints.dev_ a new request with the expected output in the request body::

   Paris: Sunny

With Remoulade, you can run this function asynchronously by using the |actor| decorator. 
We will use RabbitMQ as a message broker. 
To configure it, instantiate a |RabbitmqBroker| and set it as the global broker, then declare the ``get_weather`` actor to the broker.

.. code-block:: python
   :caption: get_weather.py
   :emphasize-lines: 2, 3, 7, 17, 18, 19

   import requests
   import remoulade
   from remoulade.brokers.rabbitmq import RabbitmqBroker

   url_endpoint = "https://www.<unique-id>.endpoints.dev/"  # put your unique endpoint here

   @remoulade.actor
   def get_weather(city):
        url = f"https://goweather.herokuapp.com/weather/{city}"

        response = requests.get(url).json()

        text = f'{city}: {response["description"]}'
        requests.post(url_endpoint, json=text)


   rabbitmq_broker = RabbitmqBroker()
   remoulade.set_broker(rabbitmq_broker)
   remoulade.declare_actors([get_weather])

It is now possible to run ``get_weather`` asynchronously on another process by calling its |send| method::


   >>> from get_weather import get_weather
   >>> get_weather.send("Lyon")
   Message(queue_name='default', actor_name='get_weather', args=('Lyon',), kwargs={}, options={}, message_id='686f9577-d5d9-4853-a2fb-66bde2e60098', message_timestamp=1625665996101)


Using the ``send`` function doesn't run the actor but instead enqueues a message to RabbitMQ. 
To process it, we have to spawn Remoulade workers by running the following command in another terminal window::

   $ source env/bin/activate
   $ remoulade get_weather

After running this command, you will see these lines::

   [<datetime>] [MainThread] [remoulade] [INFO] Worker is ready for action.
   [<datetime>] [Thread-#] [remoulade.worker.WorkerThread] [INFO] Started Actor get_weather / <message>
   [<datetime>] [Thread-#] [remoulade.worker.WorkerThread] [INFO] Finished Actor get_weather / <message> after <process-time>ms.

These lines indicate that workers have spawned and that one of them has run the function.

Using a Group of tasks
----------------------

To run multiple actors at once and gather their results or wait for all of them to finish, you can use a |group|.
For example, we can use it to get weather data for multiple cities at once::

   >>> from get_weather import get_weather
   >>> from remoulade import group
   >>> cities = ['Paris', 'Tokyo', 'Washington', 'Brasília', 'Johannesburg']
   >>> get_weather_group = group([get_weather.message(city) for city in cities])
   >>> get_weather_group.run()

This will enqueue several messages at once. You can track all the actors being run in the worker terminal.


Creating a Pipeline of tasks
----------------------------

Actors can be chained using a pipeline. 
For instance, we could refactor our ``get_weather`` function into three separate functions: one to extract weather data from the weather-api, one to transform it into the desired format, and one to upload it to endpoints.dev.

.. code-block:: python
   :caption: get_weather.py

   import requests
   import remoulade
   from remoulade.brokers.rabbitmq import RabbitmqBroker

   url_endpoint = "https://www.<unique-id>.endpoints.dev/"  # put your unique endpoint here

   @remoulade.actor
   def extract_city(city):
       url = f"https://goweather.herokuapp.com/weather/{city}"
       response = requests.get(url).json()
       response['city'] = city
       return response


   @remoulade.actor
   def transform_city(response):
       text = f'{response["city"]}: {response["description"]}'
       return text


   @remoulade.actor
   def load_city(text):
       requests.post(url_endpoint, json=text)


   rabbitmq_broker = RabbitmqBroker()
   remoulade.set_broker(rabbitmq_broker)
   remoulade.declare_actors([extract_city, transform_city, load_city])

At this point, you will need to restart your Remoulade workers in order for them to be able to process these new functions. 
These three functions can be chained together::

   >>> from remoulade import pipeline
   >>> from get_weather import extract_city, transform_city, load_city
   >>> weather_etl_pipeline = pipeline([extract_city.message("Paris"), transform_city.message(), load_city.message()])
   >>> weather_etl_pipeline.run()

This can also be done by using the pipe notation::

   >>> from get_weather import extract_city, transform_city, load_city
   >>> weather_etl_pipeline = extract_city.message("Paris") | transform_city.message() | load_city.message()
   >>> weather_etl_pipeline.run()

With both notations, the worker terminal will display new lines indicating that the three functions have successfully been run one after the other.

Using the Result Middleware
---------------------------

In this part, we will demonstrate how to use the |Results| middleware combined with Redis to gather results from an actor or a group.

First, you will need to install the dependencies for using Remoulade with Redis::

   $ pip install -U 'remoulade[redis]'

You will also need to install Redis_ or use Docker_ to run a Redis image::

   $ docker run -d --name redis -p 6379:6379 redis


Now, add the Result middleware with the Redis Backend to the broker :

.. code-block:: python
   :caption: get_weather.py
   :emphasize-lines: 4, 5, 23, 25, 26


   import requests
   import remoulade
   from remoulade.brokers.rabbitmq import RabbitmqBroker
   from remoulade.results.backends import RedisBackend
   from remoulade.results import Results


   @remoulade.actor
   def extract_city(city):
      ...


   @remoulade.actor()
   def transform_city(response):
       ...


   @remoulade.actor
   def load_city(text):
      ...


   result_backend = RedisBackend()
   rabbitmq_broker = RabbitmqBroker()
   result_time_limit_ms = 10 * 60 * 1000 # 10 mn
   rabbitmq_broker.add_middleware(Results(backend=result_backend, store_results=True, result_ttl=result_time_limit_ms))
   remoulade.set_broker(rabbitmq_broker)
   remoulade.declare_actors([extract_city, transform_city, load_city])

Two new things are happening here. 
First, we instantiate a |RedisResBackend| Result Backend that will allow us to store our data in Redis. 
Then we add a |Results| middleware that will store our results in this backend, with a defined time limit.
You will need to restart your Remoulade workers once again after adding this middleware.

We can now wait for an actor and get its results::

   >>> from get_weather import extract_city
   >>> message = extract_city.send("Paris")
   >>> result = message.result.get(block=True)
   >>> print(result)

It also allows to wait for a group of message to finish::

   >>> from get_weather import extract_city
   >>> from remoulade import group
   >>> cities = ['Paris', 'Tokyo', 'Washington', 'Brasília', 'Johannesburg']
   >>> extract_group = group([extract_city.message(city) for city in cities])
   >>> extract_group.run()
   >>> extract_group.results.wait()

Or iterate over its results::

   >>> for res in extract_group.results.get(block=True):
   ...

The Results middleware can also be used to pipe a group of actors into a single actor.
To illustrate this, we will take a more advanced example in which we want to get the weather for multiple cities and then send them to endpoints.dev with one single request instead of calling multiple times the ``load_city`` function and thus make numerous requests. 
In order to achieve this, we have to write a new function : ``load_cities``, which takes a list of text to load instead of a single text::


   @remoulade.actor
   def load_cities(city_lines):
       url_endpoint = <url_endpoint>
       requests.post(url_endpoint, json=city_lines)


After adding this new function, you will need add ``load_cities`` to the list of declared actors, and restart your Remoulade workers::

   remoulade.declare_actors([extract_city, transform_city, load_city, load_cities])

Our objective can now be achieved by running the following commands::

   >>> from get_weather import extract_city, transform_city, load_cities
   >>> from remoulade import pipeline, group
   >>> cities = ['Paris', 'Tokyo', 'Washington', 'Brasília', 'Johannesburg']
   >>> grp = group([extract_city.message(city) | transform_city.message() for city in cities])
   >>> weather_etl_pipeline = grp | load_cities.message()
   >>> weather_etl_pipeline.run()

Scheduling Messages
-------------------

Remoulade includes a scheduler that allows running actors periodically. 
We can use it here to keep our weather data updated.

Let's get back to our ``get_weather`` function and add a scheduler to update the weather data every 10 seconds.

.. code-block:: python
   :caption: get_weather.py
   :emphasize-lines: 4, 20, 21, 23

   import requests
   import remoulade
   from remoulade.brokers.rabbitmq import RabbitmqBroker
   from remoulade.scheduler import ScheduledJob, Scheduler

   url_endpoint = "https://www.<unique-id>.endpoints.dev/"  # put your unique endpoint here

   @remoulade.actor
   def get_weather(city):
        url = f"https://goweather.herokuapp.com/weather/{city}"

        response = requests.get(url).json()

        text = f'{city}: {response["description"]}'
        requests.post(url_endpoint, json=text)


   rabbitmq_broker = RabbitmqBroker()
   remoulade.set_broker(rabbitmq_broker)
   scheduler = Scheduler(rabbitmq_broker, [ScheduledJob(actor_name="get_weather", args=("Paris",), interval=10)])
   remoulade.set_scheduler(scheduler)
   remoulade.declare_actors([get_weather])
   scheduler.start()

To set up the scheduler, we instantiate it, set it as the global scheduler, and finally start it.

If you run this script and get back to the worker terminal, you will see ``get_weather`` being executed every 10 seconds.

Monitoring and Managing your tasks
----------------------------------

To monitor and manage your tasks, you can use the Superbowl_ dashboard.

.. _Superbowl: https://github.com/wiremind/super-bowl

First, you will need to install Node.js_. Then, clone Superbowl_ in another directory, install its dependencies and run it::

   $ cd ..
   $ git clone https://github.com/wiremind/super-bowl.git
   $ npm install
   $ npm run serve

.. _Node.js: https://nodejs.org/en/download/

Now, if you open ``localhost:8080`` in your browser, you will see the SuperBowl dashboard, but you will not see your messages yet. In order to see and manage them, you will have to modify the ``get_weather.py`` script to serve the remoulade api.

.. code-block:: python
   :caption: get_weather.py
   :emphasize-lines: 4, 22, 23

   import requests
   import remoulade
   from remoulade.brokers.rabbitmq import RabbitmqBroker
   from remoulade.api.main import app


   @remoulade.actor
   def get_weather(city):
       url = f"https://goweather.herokuapp.com/weather/{city}"

       response = requests.get(url).json()

       url_endpoint = <url_endpoint>
       text = f'{city}: {response["description"]}'
       requests.post(url_endpoint, json=text)


   rabbitmq_broker = RabbitmqBroker()
   remoulade.set_broker(rabbitmq_broker)
   remoulade.declare_actors([get_weather])

   if __name__ == "__main__":
       app.run(host="localhost", port=5005)

Now you can use the Enqueue tab to enqueue messages with custom arguments, and then see their progress in the messages tab. 
Additionally, if you run groups or scheduled jobs in your script, you will be able to see them in their respective tabs.

Next Steps
----------

If you want to learn more about Remoulade, you can read the :doc:`guide` and visit the other sections.
