.. _Reference API Reference:

API Reference
=============

.. module:: remoulade


Functions
---------

.. autofunction:: get_broker
.. autofunction:: set_broker
.. autofunction:: get_encoder
.. autofunction:: set_encoder


Actors & Messages
-----------------

.. autofunction:: actor
.. autoclass:: Actor
   :members:
.. autoclass:: Message
   :members:

Class-based Actors
^^^^^^^^^^^^^^^^^^

.. autoclass:: GenericActor
   :members:

Message Composition
^^^^^^^^^^^^^^^^^^^

.. autoclass:: group
   :members:
.. autoclass:: pipeline
   :members:

Message Encoders
^^^^^^^^^^^^^^^^

Encoders are used to serialize and deserialize messages over the wire.

.. autoclass:: Encoder
   :members:
.. autoclass:: JSONEncoder
.. autoclass:: PickleEncoder



Brokers
-------

.. autoclass:: Broker
   :members:
.. autoclass:: Consumer
   :members:  __iter__, __next__, ack, nack, close
.. autoclass:: MessageProxy
   :members:
.. autoclass:: remoulade.brokers.rabbitmq.RabbitmqBroker
   :members:
   :inherited-members:
.. autoclass:: remoulade.brokers.postgres.PostgresBroker
   :members:
   :inherited-members:
.. autoclass:: remoulade.brokers.stub.StubBroker
   :members:
   :inherited-members:

Postgres Queue Names
^^^^^^^^^^^^^^^^^^^^

How ``PostgresBroker`` maps a queue name to the name of its PGMQ queue and tables
(see the "Queue names" section of the user guide).

.. autodata:: remoulade.helpers.pgmq_names.QUEUE_NAME_MAX_LENGTH

   The longest pgmq name a queue may have: PGMQ's own limit, which also keeps the
   longest index name remoulade derives (``q_<pgmq name>_msg_id_idx``) under
   PostgreSQL's 63-byte identifier limit.

.. autofunction:: remoulade.helpers.pgmq_names.physical_queue_name

   Return the pgmq name of a queue, in which every dot becomes ``__``
   (``etl.low`` -> ``etl__low``). A pgmq name is its own pgmq name.

.. autofunction:: remoulade.helpers.pgmq_names.logical_queue_name

   Return the queue name of a pgmq name, the inverse of ``physical_queue_name`` for
   every valid queue name.

.. autofunction:: remoulade.helpers.pgmq_names.pgmq_table_name

   Return the name of the PGMQ table (in the ``pgmq`` schema) holding the messages
   (``prefix="q"``) or the archive (``prefix="a"``) of a queue, given its queue name
   or its pgmq name.

.. autofunction:: remoulade.helpers.pgmq_names.assert_valid_queue_name

   Raise ``ValueError`` unless a queue name can be declared on a ``PostgresBroker``:
   it matches :data:`~remoulade.actor.QUEUE_NAME_PATTERN`, holds no uppercase letter,
   contains neither ``__``, ``_.`` nor ``._``, and its pgmq name is at most
   ``QUEUE_NAME_MAX_LENGTH`` characters long.

.. autofunction:: remoulade.helpers.pgmq_names.assert_valid_pgmq_queue_name

   Raise ``ValueError`` unless a name is the pgmq name of a valid queue name, which
   is what ``RemouladePostgresClient.create_indexes`` and
   ``RemouladePostgresClient.patch_headers`` take.


.. _Reference Middleware:

Middleware
----------

The following middleware are all enabled by default.

.. autoclass:: Middleware
   :members:
   :member-order: bysource
.. autoclass:: remoulade.middleware.AgeLimit
.. autoclass:: remoulade.middleware.CatchError
.. autoclass:: remoulade.middleware.Pipelines
.. autoclass:: remoulade.middleware.Prometheus
.. autoclass:: remoulade.middleware.Retries
.. autoclass:: remoulade.middleware.ShutdownNotifications
.. autoclass:: remoulade.middleware.TimeLimit

Middleware Errors
^^^^^^^^^^^^^^^^^

The class hierarchy for middleware exceptions:

.. code-block:: none

    BaseException
    +-- Exception
    |   +-- remoulade.middleware.MiddlewareError
    |       +-- remoulade.middleware.SkipMessage
    +-- remoulade.middleware.Interrupt
        +-- remoulade.middleware.Shutdown
        +-- remoulade.middleware.TimeLimitExceeded


.. autoclass:: remoulade.middleware.MiddlewareError
.. autoclass:: remoulade.middleware.SkipMessage
.. autoclass:: remoulade.middleware.Interrupt
.. autoclass:: remoulade.middleware.TimeLimitExceeded
.. autoclass:: remoulade.middleware.Shutdown


Results
-------

Actor results can be stored and retrieved by leveraging result
backends and the results middleware.  Results and result backends are
not enabled by default and you should avoid using them until you have
a really good use case.  Most of the time you can get by with actors
simply updating data in your database instead of using results.

Result objects
^^^^^^^^^^^^^^

.. autoclass:: remoulade.result.Result
.. autoclass:: remoulade.collection_results.CollectionResults


Result Middleware
^^^^^^^^^^^^^^^^^

.. autoclass:: remoulade.results.Results

Backends
^^^^^^^^

.. autoclass:: remoulade.results.ResultBackend
.. autoclass:: remoulade.results.backends.RedisBackend
.. autoclass:: remoulade.results.backends.StubBackend


Rate Limiting
-------------

Rate limits can be enforced at enqueue time and/or process time using
dedicated middleware and a shared backend.

Middleware
^^^^^^^^^^

.. autoclass:: remoulade.rate_limits.middleware.RateLimitEnqueue
.. autoclass:: remoulade.rate_limits.middleware.RateLimitProcess

Rate Limit Backends
^^^^^^^^^^^^^^^^^^^

.. autoclass:: remoulade.rate_limits.RateLimitBackend
.. autoclass:: remoulade.rate_limits.backends.RedisBackend
.. autoclass:: remoulade.rate_limits.backends.StubBackend


Concurrency
-----------

Concurrency limits are enforced during message processing using the
Concurrent middleware and a backend that coordinates leases.

Middleware
^^^^^^^^^^

.. autoclass:: remoulade.concurrent.middleware.Concurrent

Concurrency Backends
^^^^^^^^^^^^^^^^^^^^

.. autoclass:: remoulade.concurrent.ConcurrencyBackend
.. autoclass:: remoulade.concurrent.backends.RedisBackend
.. autoclass:: remoulade.concurrent.backends.StubBackend
.. autoclass:: remoulade.concurrent.lease.Lease


Workers
-------

.. autoclass:: Worker
   :members:


Errors
------

.. autoclass:: RemouladeError
   :members:
.. autoclass:: BrokerError
   :members:
.. autoclass:: ActorNotFound
   :members:
.. autoclass:: QueueNotFound
   :members:
.. autoclass:: ConnectionError
   :members:
.. autoclass:: ConnectionClosed
   :members:
.. autoclass:: ConnectionFailed
   :members:
.. autoclass:: remoulade.concurrent.ConcurrencyLimitExceeded
   :members:
.. autoclass:: RateLimitExceeded
   :members:
.. autoclass:: NoResultBackend
   :members:
.. autoclass:: remoulade.errors.NoRateLimitBackend
   :members:
.. autoclass:: remoulade.errors.NoConcurrencyBackend
   :members:
.. autoclass:: remoulade.results.ResultError
   :members:
.. autoclass:: remoulade.results.ResultMissing
   :members:
.. autoclass:: remoulade.results.ResultTimeout
   :members:
