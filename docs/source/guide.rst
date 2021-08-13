.. include:: global.rst

User Guide
==========

To follow along with this guide you'll need to install and run RabbitMQ_
and then set up a new `virtual environment`_ in which you'll have
to install Remoulade and Requests_::

  $ pip install 'remoulade[rabbitmq]' requests

.. _requests: http://docs.python-requests.org
.. _virtual environment: http://docs.python-guide.org/en/latest/starting/install3/osx/#virtual-environments


Actors
------

As a quick and dirty example of a task that's worth processing in the
background, let's write a function that counts the "words" at a
particular URL:

.. code-block:: python
   :caption: count_words.py

   import requests

   def count_words(url):
     response = requests.get(url)
     count = len(response.text.split(" "))
     print(f"There are {count} words at {url!r}.")

There's not a ton going on here.  We just grab the response content at
that URL and print out how many space-separated chunks there are in
that response.  Sure enough, running this in the interactive
interpreter yields about what we expect::

  >>> from count_words import count_words
  >>> count_words("http://example.com")
  There are 338 words at 'http://example.com'.

To turn this into a function that can be processed asynchronously
using Remoulade, all we have to do is decorate it with |actor|
and declare it to the broker:

.. code-block:: python
   :caption: count_words.py
   :emphasize-lines: 1, 4

   import remoulade
   import requests
   from remoulade import get_broker()

   @remoulade.actor
   def count_words(url):
     response = requests.get(url)
     count = len(response.text.split(" "))
     print(f"There are {count} words at {url!r}.")

   remoulade.declare_actors([count_words])

Like before, if we call the function in the interactive interpreter,
it will run synchronously and we'll get the same result out::

  >>> count_words("http://example.com")
  There are 338 words at 'http://example.com'.

What's changed is we're now able to tell the function to run
asynchronously by calling its |send| method::

  >>> count_words.send("http://example.com")
  Message(
    queue_name='default',
    actor_name='count_words',
    args=('http://example.com',), kwargs={}, options={},
    message_id='8cdcae57-af36-40ba-9616-849a336a4316',
    message_timestamp=1498557015410)

Doing so immediately enqueues a message (via our local RabbitMQ
server) that can be processed asynchronously but *doesn't actually run
the function*.  In order to run it, we'll have to boot up a Remoulade
worker.

.. note::
   Because all messages have to be sent over the network, any
   arguments you send to an actor must be JSON-encodable.


Workers
-------

Remoulade comes with a command line utility called, predictably,
``remoulade``.  This utility is able to spin up multiple concurrent
worker processes that pop messages off the queue and send them to
actor functions for execution.

To spawn workers for our ``count_words.py`` example, run the following
command in a new terminal window::

  $ remoulade count_words

This will spin up a process on your machine with 8 worker threads per process.
Run ``remoulade -h`` if you want to see a list of the available command line flags.

As soon as you run that command you'll see log output along these
lines::

  [2017-11-19 13:03:48,188] [PID 22370] [MainThread] [remoulade.MainProcess] [INFO] Remoulade '0.13.1' is booting up.
  [2017-11-19 13:03:48,349] [PID 22377] [MainThread] [remoulade.WorkerProcess(3)] [INFO] Worker process is ready for action.
  [2017-11-19 13:03:48,350] [PID 22375] [MainThread] [remoulade.WorkerProcess(1)] [INFO] Worker process is ready for action.
  [2017-11-19 13:03:48,357] [PID 22376] [MainThread] [remoulade.WorkerProcess(2)] [INFO] Worker process is ready for action.
  [2017-11-19 13:03:48,357] [PID 22374] [MainThread] [remoulade.WorkerProcess(0)] [INFO] Worker process is ready for action.
  [2017-11-19 13:03:48,358] [PID 22379] [MainThread] [remoulade.WorkerProcess(5)] [INFO] Worker process is ready for action.
  [2017-11-19 13:03:48,362] [PID 22381] [MainThread] [remoulade.WorkerProcess(7)] [INFO] Worker process is ready for action.
  [2017-11-19 13:03:48,364] [PID 22380] [MainThread] [remoulade.WorkerProcess(6)] [INFO] Worker process is ready for action.
  [2017-11-19 13:03:48,366] [PID 22378] [MainThread] [remoulade.WorkerProcess(4)] [INFO] Worker process is ready for action.
  [2017-11-19 13:03:48,369] [PID 22377] [Thread-4] [count_words.count_words] [INFO] Received args=('http://example.com',) kwargs={}.
  There are 338 words at 'http://example.com'.
  [2017-11-19 13:03:48,679] [PID 22377] [Thread-4] [count_words.count_words] [INFO] Completed after 310.42ms.

If you open your Python interpreter back up and send the actor some
more URLs to process::

  >>> urls = [
  ...   "https://news.ycombinator.com",
  ...   "https://xkcd.com",
  ...   "https://rabbitmq.com",
  ... ]
  >>> [count_words.send(url) for url in urls]
  [Message(queue_name='default', actor_name='count_words', args=('https://news.ycombinator.com',), kwargs={}, options={}, message_id='a99a5b2d-d2da-407b-be55-f2925266e216', message_timestamp=1498557998218),
   Message(queue_name='default', actor_name='count_words', args=('https://xkcd.com',), kwargs={}, options={}, message_id='0ec93dcb-2f9f-414f-99ec-7035e3b1ac5a', message_timestamp=1498557998218),
   Message(queue_name='default', actor_name='count_words', args=('https://rabbitmq.com',), kwargs={}, options={}, message_id='d3dd9799-1ea5-4b00-a70b-2cd6f6f634ed', message_timestamp=1498557998218)]

and then switch back to the worker terminal, you'll see nine new
lines::

  [2017-11-19 13:10:02,620] [PID 24357] [Thread-4] [count_words.count_words] [INFO] Received args=('https://rabbitmq.com',) kwargs={}.
  [2017-11-19 13:10:02,621] [PID 24357] [Thread-6] [count_words.count_words] [INFO] Received args=('https://xkcd.com',) kwargs={}.
  [2017-11-19 13:10:02,621] [PID 24357] [Thread-5] [count_words.count_words] [INFO] Received args=('https://news.ycombinator.com',) kwargs={}.
  There are 888 words at 'https://rabbitmq.com'.
  [2017-11-19 13:10:02,757] [PID 24357] [Thread-4] [count_words.count_words] [INFO] Completed after 137.26ms.
  There are 461 words at 'https://xkcd.com'.
  [2017-11-19 13:10:02,841] [PID 24357] [Thread-6] [count_words.count_words] [INFO] Completed after 219.76ms.
  There are 3598 words at 'https://news.ycombinator.com'.
  [2017-11-19 13:10:03,297] [PID 24357] [Thread-5] [count_words.count_words] [INFO] Completed after 675.19ms.

At this point, you're probably wondering what happens if you send the
actor an invalid URL.  Let's try it::

  >>> count_words.send("foo")


Message Retries
---------------

If an error occurs during message processing, it will be terminated with a failure message.
Alternatively, you can add the |Retries| Middleware to the broker and set the max_retries or retry_when option to automatically retry your message on failure.

You can specify how failures should be retried on a per-actor basis.
For example, if you want to limit the maximum number of retries for
``count_words`` you can pass the ``max_retries`` keyword argument to
|actor|::

  @remoulade.actor(max_retries=3)
  def count_words(url):
    ...

If you want to retry certain exceptions and not others, you can pass a
predicate function via the ``retry_when`` parameter::

  def should_retry(retries_so_far, exception):
    return retries_so_far < 3 and isinstance(exception, HttpTimeout)

  @remoulade.actor(retry_when=should_retry)
  def count_words(url):
    ...

If you want to use a different strategy than the default exponential backoff to define how long to wait between retries, you can pass the `backoff_strategy` keyword argument to |actor|. For instance to retry every minute::

   @remoulade.actor(min_backoff=60000, backoff_strategy='constant')
   def count_words(URL):
     ...

The following retry options are configurable on a per-actor basis:

max_retries
^^^^^^^^^^^

The maximum number of times a message should be retried. Default to ``0``.

min_backoff
^^^^^^^^^^^

The minimum number of milliseconds of backoff to apply between retries.  Must be greater than 100 milliseconds. Defaults to 15 seconds.

max_backoff
^^^^^^^^^^^

The maximum number of milliseconds of backoff to apply between retries. Higher values are less reliable. Defaults to 1 hour.

retry_when
^^^^^^^^^^

A callable that takes the number of retries so far and the exception as an input, and expects a boolean that determines whether or not the message will be retried as an output.  When this is set, ``max_retries`` is ignored. Defaults to ``None``.

backoff_strategy
^^^^^^^^^^^^^^^^

The strategy used to compute the backoff. Defaults to ``exponential``. The available strategies are :

* ``constant``: constant backoff, equal to min_backoff.
* ``linear``: linear backoff, starting from min_backoff.
* ``spread_linear``: linear backoff, linearly spread between min_backoff and max_backoff.
* ``exponential``: exponential backoff, starting from min_backoff.
* ``spread_exponential``: exponential backoff, exponentially spread between min_backoff and max_backoff.

jitter
^^^^^^

When True, a small random value will be added to the backoff to avoid mass simultaneous retries. Defaults to ``True``.

Message Age Limits
------------------

Instead of limiting the number of times messages can be retried, you
might want to expire old messages.  You can specify the ``max_age`` of
messages (given in milliseconds) on a per-actor basis::

  @remoulade.actor(max_age=3600000)
  def count_words(url):
    ...

Dead Letters
^^^^^^^^^^^^

Once a message has exceeded its retry or age limits, it gets moved to
the dead letter queue where it's kept for up to 7 days and then
automatically dropped from the message broker.  From here, you can
manually inspect the message and decide whether or not it should be
put back on the queue.


Message Time Limits
-------------------

In ``count_words``, we didn't set an explicit timeout for the outbound
request which means that it can take a very long time to complete if
the server we're requesting is timing out.  Remoulade has a default
actor time limit of 30 minutes, which means that any actor running for
longer than 30 minutes is killed with a |TimeLimitExceeded| error.

You can control these time limits at the individual actor level by
specifying the ``time_limit`` (in milliseconds) of each one::

  @remoulade.actor(time_limit=60000)
  def count_words(url):
    ...

.. note::
   While this will keep our actor from running forever, remember that
   you should take care to always specify a timeout for the request
   itself, and this is **not** a good way to handle request timeouts
   in production code.

.. warning::
   Time limits are best-effort.  They cannot cancel system calls or
   any function that doesn't currently hold the GIL under CPython.

   For more information, see the section on :ref:`message-interrupts`.

.. note::
   If time limit fail to stop the execution via |TimeLimitExceeded| (see warning),
   a SIGKILL will be sent to the worker after 10 seconds (by default).
   This delay can be set with the ``sigkill_delay`` of |TimeLimit|,
   or feature can be disabled by setting ``sigkill_delay`` to ``None``.


Handling Time Limits
^^^^^^^^^^^^^^^^^^^^

If you want to gracefully handle time limits within an actor, you can
wrap its source code in a try block and catch |TimeLimitExceeded|::

  from remoulade.middleware import TimeLimitExceeded

  @remoulade.actor(time_limit=1000)
  def long_running():
    try:
      setup_missiles()
      time.sleep(2)
      launch_missiles()    # <- this will not run
    except TimeLimitExceeded:
      teardown_missiles()  # <- this will run


Scheduling Messages
-------------------

You can schedule messages to run some time in the future by calling
|send_with_options| on actors and providing a ``delay`` (in
milliseconds)::

  >>> count_words.send_with_options(args=("https://example.com",), delay=10000)
  Message(
    queue_name='default',
    actor_name='count_words',
    args=('https://example.com',), kwargs={},
    options={'eta': 1498560453548},
    message_id='7387dc76-8ebe-426e-aec1-db34c236563c',
    message_timestamp=1498560443548)

Keep in mind that *your message broker is not a database*.  Scheduled
messages should represent a small subset of all your messages.


Prioritizing Messages
---------------------

Say your app has some actors that are higher priority than others: for
example, actors that affect your UI and make users wait, or are
otherwise user-facing, versus actors that aren't.  When choosing
between two concurrent messages to run, Remoulade will run the Message
that belongs to the actor with the highest priority.

You can set an Actor's priority via the ``priority`` keyword argument::

  @remoulade.actor(priority=1)
  def generate_report(user_id):
    ...

  @remoulade.actor(priority=0) # 0 is the default
  def sync_order_to_warehouse(order_id):
    ...

That way if both ``generate_report`` and ``sync_order_to_warehouse``
are scheduled to run at the same time but there's only capacity to run
one of them, ``generate_report`` will always run *first*.

Although all positive integers represent valid priorities, if you're
going to use this feature, I'd recommend setting up constants for the
various priorities you plan to use:

  PRIO_LO = 0
  PRIO_MED = 1
  PRIO_HI = 2

Rabbitmq also have a support for priorities, to take advantage of it
you need to set ``max_priority``

   broker = RabbitmqBroker(url="rabbitmq", max_priority=PRIO_HI)

In `priority documentation`_, you will see that recommended value for
max_priority is between 1 and 10 (max: 255).
You should try to use the minimum max_priority possible and not use priority
values bigger than max_priority as they will be considered as max_priority.

.. important::
   The bigger the numeric value, the higher priority!


Message Brokers
---------------

Remoulade abstracts over the notion of a message broker and currently
supports RabbitMQ out of the box.

RabbitMQ Broker
^^^^^^^^^^^^^^^

To configure the RabbitMQ host, instantiate a |RabbitmqBroker| and set
it as the global broker as early as possible during your program's
execution::

  import remoulade

  from remoulade.brokers.rabbitmq import RabbitmqBroker

  rabbitmq_broker = RabbitmqBroker(url="rabbitmq")
  remoulade.set_broker(rabbitmq_broker)


Local Broker
^^^^^^^^^^^^^^^

If you just want to execute the actors when the message is enqueued
without running any Worker (for example in a development environment)::

  import remoulade

  from remoulade.brokers.local import LocalBroker
  from remoulade.results.backends import LocalBackend
  from remoulade.cancel.backends import StubBackend

  local_broker = LocalBroker(middleware=[])
  broker.add_middleware(Results(backend=LocalBackend()))
  broker.add_middleware(Cancel(backend=StubBackend()))
  remoulade.set_broker(local_broker)


Unit Testing
------------

Remoulade provides a |StubBroker| that can be used in unit tests so you
don't have to have a running RabbitMQ or Redis instance in order to
run your tests.  My recommendation is to use it in conjunction with
`pytest fixtures`_:

.. code-block:: python
   :caption: broker.py

   import os

   from remoulade.brokers.rabbitmq import RabbitmqBroker
   from remoulade.brokers.stub import StubBroker

   if os.getenv("UNIT_TESTS") == "1":
     broker = StubBroker()
     broker.emit_after("process_boot")
   else:
     broker = RabbitmqBroker()

.. code-block:: python
   :caption: conftest.py

   import remoulade
   import pytest

   from remoulade import Worker
   from yourapp import broker

   @pytest.fixture()
   def stub_broker():
     broker.flush_all()
     return broker

   @pytest.fixture()
   def stub_worker():
     worker = Worker(broker, worker_timeout=100)
     worker.start()
     yield worker
     worker.stop()

Then you can inject and use those fixtures in your tests::

  def test_count_words(stub_broker, stub_worker):
    count_words.send("http://example.com")
    stub_broker.join(count_words.queue_name)
    stub_worker.join()

Because all actors are callable, you can of course also unit test them
synchronously by calling them as you would normal functions.


.. _pytest fixtures: https://docs.pytest.org/en/latest/fixture.html
.. _priority documentation: https://www.rabbitmq.com/priority.html

