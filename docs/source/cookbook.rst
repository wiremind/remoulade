.. include:: global.rst

Cookbook
========

This part of the docs contains recipes for various things you might
want to do using Remoulade.  Each section will be light on prose and
code-heavy, so if you have any questions about one of the recipes,
open an `issue on GitHub`_.

.. _issue on GitHub: https://github.com/wiremind/remoulade/issues


Callbacks
---------

Remoulade has built-in support for sending actors messages when other
actors succeed or fail.  The ``on_failure`` callback is called every
time an actor fails, even if the message is going to be retried.

.. code-block:: python

   import remoulade

   @remoulade.actor
   def identity(x):
     return x

   @remoulade.actor
   def print_result(message_data, result):
     print(f"The result of message {message_data['message_id']} was {result}.")

   @remoulade.actor
   def print_error(message_data, exception_data):
     print(f"Message {message_data['message_id']} failed:")
     print(f"  * type: {exception_data['type']}")
     print(f"  * message: {exception_data['message']!r}")

   if __name__ == "__main__":
     identity.send_with_options(
       args=(42,)
       on_failure=print_error,
       on_success=print_result,
     )


Composition
-----------

Remoulade has built-in support for a couple of high-level composition
constructs.  You can use these to combine generalized tasks that don't
know about one another into complex workflows.

In order to take advantage of group and pipeline result management,
you need to enable result storage and your actors need to store
results.  Check out the `Results`_ section for more information on
result storage.

Groups
^^^^^^

|Groups| run actors in parallel and let you gather their results or
wait for all of them to finish.  Assuming you have a computationally
intensive actor called ``frobnicate``, you can group multiple
messages together as follows::

  g = group([
    frobnicate.message(1, 2),
    frobnicate.message(2, 3),
    frobnicate.message(3, 4),
  ]).run()

This will enqueue 3 separate messages and, assuming there are enough
resources available, execute them in parallel.  You can then wait for
the whole group to finish::

  g.results.wait(timeout=10_000)  # 10s expressed in millis

Or you can iterate over the results::

  for res in g.results.get(block=True, timeout=5_000):
    ...

Results are returned in the same order that the messages were added to
the group.
If you don't pass the ``timeout`` argument in ``get``, the timeout will have a default value of 10 seconds.
To set a custom default timeout, pass a ``default_timeout`` argument when instantiating your result backend.

Pipelines
^^^^^^^^^

Actors can be chained together using the |pipeline| function. For
example, if you have an actor that gets the text contents of a website
and one that counts the number of "words" in a piece of text:

.. code-block:: python

   @remoulade.actor
   def get_uri_contents(uri):
     return requests.get(uri).text

   @remoulade.actor
   def count_words(uri, text):
     count = len(text.split(" "))
     print(f"There are {count} words at {uri}.")

You can chain them together like so::

  uri = "http://example.com"
  pipe = pipeline([
    get_uri_contents.message(uri),
    count_words.message(uri),
  ]).run()

Or you can use pipe notation to achieve the same thing::

  pipe = get_uri_contents.message(uri) | count_words.message(uri)

In both cases, the result of running ``get_uri_contents(uri)`` is
passed as the last positional argument to ``count_words``.  If you
would like to avoid passing the result of an actor to the next one in
line, set the ``pipe_ignore`` option to ``True`` when you create the
"receiving" message::

  (
    bust_caches.message() |
    prepare_codes.message_with_options(pipe_ignore=True) |
    launch_missiles.message()
  )

Here, the result of ``bust_caches()`` will not be passed to
``prepare_codes()``, but the result of ``prepare_codes()`` will be
passed to ``launch_missiles(codes)``.  To get the end result of a
pipeline -- that is, the result of the last actor in the pipeline --
you can call |pipeline_result_get|::

  pipe.result.get(block=True, timeout=5_000)

To get the intermediate results of each step in the pipeline, you can
call |pipeline_results_get|::

  for res in pipe.results.get(block=True):
    ...


Logging
-------

If you want to track your messages, you can use the |LoggingMetadata| middleware.

This middleware enables you to pass metadata to your messages, either by using the logging_metadata option::

    message = actor.message_with_options(logging_metadata={"id":"value"})

Or by passing a callback that returns the metadata to the message using the logging_metadata_getter option::

    def callback():
        return {"id":"value}

    message = actor.message_with_options(logging_metadata_getter=callback)

Either way, the logging_metadata will be sent in all remoulade logs concerning this message, and can also be accessed like this::

    message.options['logging_metadata']

As with most options, you can pass these options at every level : message, actor and middleware.

When a message is created, the value of the logging_metadata and return value of logging_metadata from every level are merged and passed to the message.
Same fields in multiple levels or options are overwritten following the standard option priority : message level having higher priority than actor level, which has higher priority that middleware level. For each level, logging_metadata_getter has higher priority that logging_metadata.

.. note::
    Because they are already used in logging, "message_id" and "input" cannot be used as fields in logging_metadata.

Error Reporting
---------------

Reporting errors with Rollbar
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Rollbar_ provides an easy-to-use Python client.  Add it to your
project with pipenv_::

   $ pipenv install rollbar

Save the following middleware to a module inside your project:

.. code-block:: python

   import remoulade
   import rollbar

   class RollbarMiddleware(remoulade.Middleware):
     def after_process_message(self, broker, message, *, result=None, exception=None):
       if exception is not None:
         rollbar.report_exc_info()

Finally, instantiate and add it to your broker:

.. code-block:: python

   rollbar.init(YOUR_ROLLBAR_KEY)
   broker.add_middleware(path.to.RollbarMiddleware())


.. _pipenv: https://docs.pipenv.org
.. _Rollbar: https://github.com/rollbar/pyrollbar#quick-start

Reporting errors with Sentry
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Install Sentry's raven_ client with pipenv_::

   $ pipenv install raven

Save the following middleware to a module inside your project:

.. code-block:: python

   import remoulade

   class SentryMiddleware(remoulade.Middleware):
     def __init__(self, raven_client):
       self.raven_client = raven_client

     def after_process_message(self, broker, message, *, result=None, exception=None):
       if exception is not None:
         self.raven_client.captureException()

Finally, instantiate and add it to your broker:

.. code-block:: python

   from raven import Client

   raven_client = Client(YOUR_DSN)
   broker.add_middleware(path.to.SentryMiddleware(raven_client))


.. _pipenv: https://docs.pipenv.org
.. _raven: https://github.com/getsentry/raven-python


Operations
----------

Binding Worker Groups to Queues
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By default, Remoulade workers consume all declared queues, but it's
common to want to bind worker groups to specific queues in order to
have better control over throughput.  For example, given the following
actors::

   @remoulade.actor
   def very_slow():
     ...

   @remoulade.actor(queue_name="ui-blocking")
   def very_important():
     ...

You may want to run one group of workers that only processes messages
on the ``default`` queue and another that only processes messages off
of the ``ui-blocking`` queue.  To do that, you have to pass each group
the appropriate queue on the command line:

.. code-block:: bash

   # Only consume the "default" queue
   $ remoulade an_app --queues default

   # Only consume the "ui-blocking" queue
   $ remoulade an_app --queues ui-blocking

Messages sent to ``very_slow`` will always be delievered to those
workers that consume the ``default`` queue and messages sent to
``very_important`` will always be delievered to the ones that consume
the ``ui-blocking`` queue.

Rate Limiting
-------------

Rate limiting work
^^^^^^^^^^^^^^^^^^

You can use Remoulade's |RateLimiters| to constrain actor concurrency.

.. code-block:: python

   import remoulade
   import time

   from remoulade.rate_limits import ConcurrentRateLimiter
   from remoulade.rate_limits.backends import RedisBackend

   backend = RedisBackend()
   DISTRIBUTED_MUTEX = ConcurrentRateLimiter(backend, "distributed-mutex", limit=1)

   @remoulade.actor
   def one_at_a_time():
     with DISTRIBUTED_MUTEX.acquire():
       time.sleep(1)
       print("Done.")

Whenever two ``one_at_a_time`` actors run at the same time, one of
them will be retried with exponential backoff.  This works by raising
an exception and relying on the built-in Retries middleware to do the
work of re-enqueueing the task.

If you want rate limiters not to raise an exception when they can't be
acquired, you should pass ``raise_on_failure=False`` to ``acquire``::

  with DISTRIBUTED_MUTEX.acquire(raise_on_failure=False) as acquired:
    if not acquired:
      print("Lock could not be acquired.")
    else:
      print("Lock was acquired.")


Results
-------

Storing message results
^^^^^^^^^^^^^^^^^^^^^^^

You can use Remoulade's result backends to store and retrieve message
return values.  To enable result storage, you need to instantiate and
add the |Results| middleware to your broker.

.. code-block:: python

   import remoulade

   from remoulade.brokers.rabbitmq import RabbitmqBroker
   from remoulade.results.backends import RedisBackend
   from remoulade.results import Results

   result_backend = RedisBackend()
   broker = RabbitmqBroker()
   broker.add_middleware(Results(backend=result_backend))
   remoulade.set_broker(broker)

   @remoulade.actor(store_results=True)
   def add(x, y):
     return x + y

   broker.declare_actor(add)

   if __name__ == "__main__":
     message = add.send(1, 2)
     print(message.result.get(block=True, raise_on_error=True, forget=False))

Getting a result raises |ResultMissing| when a result hasn't been
stored yet or if it has already expired (results expire after 10
minutes by default).  When the ``block`` parameter is ``True``,
|ResultTimeout| is raised instead. When the ``forget`` parameter
is ``True`` the result will be deleted from the backend when retrieved.

If an exception is raised during message execution, a serialized version
of the exception is stored in the |ResultBackend|. If ``raise_on_error``
parameter is ``True``, an |ErrorStored| is raised when it's the case.

Result
^^^^^^
.. code-block:: python

   import remoulade

   from remoulade.brokers.rabbitmq import RabbitmqBroker
   from remoulade.results.backends import RedisBackend
   from remoulade.results import Results

   result_backend = RedisBackend()
   broker = RabbitmqBroker()
   broker.add_middleware(Results(backend=result_backend))
   remoulade.set_broker(broker)

   @remoulade.actor(store_results=True)
   def add(x, y):
     return x + y

   broker.declare_actor(add)

   if __name__ == "__main__":
     message = add.send(1, 2)
     result = Result(message_id=message.message_id)
     print(result.get(block=True))


The property result of |Message| return a |Result| instance which can be used to get the result.
But you can also create a |Result| from a message_id and access to the result the same way.

Group results
^^^^^^^^^^^^^

The property ``results`` of |Group| return a |CollectionResults| instance which can be used to get the result.
You access the results with the get method, but also the completed_count of the group (the count of finished
actors errored or not).

You can also get all the message ids of a group with the message_ids property and create a |CollectionResults|
with the from_message_ids method.


Pipelines of groups
^^^^^^^^^^^^^^^^^^^

If you activated the result backend, remoulade can be used to create a group pipeline as follow::

  group_pipeline = group([
    do_something.message(1, 2),
    do_something.message(2, 3),
    do_something.message(3, 4),
  ]) | merge_results.message()

This can be handy to merge the results of parallel calculation. Under the hood, each group get a group_id and
after each actor is finished a counter in the results backend associed to the group id is incremented. If the
counter reach the number of message in the group, the results of each message are fetched from the result backend
and the next message is enqueued.

Scheduling
----------

Scheduling messages
^^^^^^^^^^^^^^^^^^^
There is a scheduler integrated into remoulade:

.. code-block:: python

    import remoulade
    from datetime import datetime
    from remoulade.brokers.rabbitmq import RabbitmqBroker
    from remoulade.scheduler import ScheduledJob, Scheduler

    broker = RabbitmqBroker()

    remoulade.set_broker(broker)
    remoulade.set_scheduler(
        Scheduler(
            broker,
            [
                ScheduledJob(actor_name="count_words", interval=86400),
            ]
        )
    )

    broker.declare_actor(count_words)


Optimizing
----------

Prefetch Limits
^^^^^^^^^^^^^^^

The prefetch count is the number of message a worker can reserve for itself
(the limit of unacknowledged message it can get from RabbitMQ).

The prefetch count is set by multiplying the prefetch_multiplier with the number
of worker threads (default: 2)

If you have many actors with a long duration you want the multiplier value to be one,
it’ll only reserve one task per worker process at a time.

But if you have short actors, you may want to increase this multiplier to reduce I/O.

.. code-block:: bash

    remoulade package --prefetch-multiplier 1


Cancel
------

Cancel a message
^^^^^^^^^^^^^^^^

You can cancel messages if you add the |Cancel| middleware to the broker.

.. code-block:: python

   import remoulade

   from remoulade.brokers.rabbitmq import RabbitmqBroker
   from remoulade.cancel.backends import RedisBackend
   from remoulade.cancel import Cancel
   from remoulade import group

   result_backend = RedisBackend()
   broker = RabbitmqBroker()
   broker.add_middleware(Results(backend=result_backend))
   remoulade.set_broker(broker)

   @remoulade.actor()
   def add(x, y):
     return x + y

   broker.declare_actor(add)

   if __name__ == "__main__":
     message = add.send(1, 2)
     message.cancel()

     g = group([add.message(1, 2), add.message(1, 2)]).run()
     g.cancel()

If a message has not yet started its processing, remoulade will not
start the execution of the actor.
Basically, the id of the message to cancel is stored in a redis set.
And before each message processing, remoulade check if the message_id
is in the set.
|message|, |group| and |pipeline| can be canceled.


Progress bar
------------

tqdm_ is the recommended way if you want to make a progress bar for a |group|:

.. code-block:: python

    from time import time, sleep
    import logging

    from tqdm import tqdm
    import remoulade

    logger = logging.getLogger(__name__)

    def blocking_remoulade_group(remoulade_group, timeout=1800):
        actor_count = len(remoulade_group)
        logger.info('Start group')
        start_time = time()
        try:
            with tqdm(total=actor_count) as progress_bar:
                results = remoulade_group.run().results
                completed_count, waited_time = 0, 0
                while waited_time < timeout and completed_count != actor_count:
                    completed_count = results.completed_count
                    waited_time = time() - start_time
                    progress_bar.update(completed_count - progress_bar.n)
                    sleep(1)

                progress_bar.update(completed_count - progress_bar.n)  # final update of the progress

            if waited_time > timeout:
                raise Exception('The operation timed out')

        except:
            logger.error('Group canceled ')
            remoulade_group.cancel()
            raise

        logger.info('Finished group')

        return results

.. _tqdm: https://tqdm.github.io/
