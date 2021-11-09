.. include:: global.rst

Changelog
=========

All notable changes to this project will be documented in this file.

`0.42.3`_ -- 2021-11-09
-----------------------
Changed
^^^^^
* api: /messages/requeue route is now POST instead of GET
* api: /messages/requeue route will now return 400 status code if there is a State middleware and the given id does not match any message's id
* api: trying to enqueue a message or add or update a scheduled job with an actor name that doesn't match an existing actor's name will return a 400 status code
* state: renamed get_states and get_states_count argument selected_ids to selected_message_ids
* state: add a selected_composition_ids to get_states and get_states_count to filter
* api: passing a timezone aware datetime in a job will now return a 400 status code

`0.42.2`_ -- 2021-11-05
-----------------------
Added
^^^^^^^
* api: add swagger to the api

`0.42.1`_ -- 2021-10-29
-----------------------
Added
^^^^^
* broker: add all relevant events to the local broker

`0.42.0`_ -- 2021-10-28
-----------------------
Changed
^^^^^^^
* cancel: when any message of a composition fails with cancel_on_error set to True, the whole composition will now be canceled
* compositions: cancel_on_error can now be set on a pipeline as well by passing it to the constructor

`0.41.1`_ -- 2021-10-28
-----------------------
Fixed
^^^^^
* state: fix sorting by column in Postgres Backend get_states

Changed
^^^^^^^
* api : removed the possibility of sorting states by args, kwargs and options in /messages/states route

`0.41.0`_ -- 2021-10-22
-----------------------
Added
^^^^^
* compositions: add a composition id to messages to identify which pipeline or group they belong to.
* state : the state middleware and api will no longer return incomplete compositions

Removed
^^^^^^^
* state: removed stub and redis backends

`0.40.2`_ -- 2021-09-29
-----------------------
Fixed
^^^^^
* api: Fixed scheduler api schema

`0.40.1`_ -- 2021-09-29
-----------------------
Added
^^^^^
* states: Added queue_name to states

`0.40.0`_ -- 2021-09-29
-----------------------
Added
^^^^^
* api: Add new routes to create, update and delete scheduled jobs

`0.39.3`_ -- 2021-09-21
-----------------------
Changed
^^^^^
* state: the 'Pending' state will now be saved before enqueueing instead of after. If the enqueue fails, the state will be marked as failed after the enqueueing.

`0.39.2`_ -- 2021-09-20
-----------------------
Changed
^^^^^
* middleware: when adding a middleware to a broker that already has a middleware of this type, it will now replace the middleware.

`0.39.1`_ -- 2021-09-17
-----------------------
Fixed
^^^^^
* states: states data will no longer be deleted on Postgres State Backend initialisation when database is already in correct version

`0.39.0`_ -- 2021-09-16
-----------------------
Added
^^^^^
* state: add new Postgres State Backend
* compose: add new docker-compose file to easily run all the services necessary to remoulade
* api: add new /messages/states DELETE route to delete states from the Postgres State Backend
* api: add new arguments to /messages/states and /groups route to filter results by actor, status, id, start_datetime and end_datetime with Postgres State Backend.

Changed
^^^^^^^
* api: change /messages/states and /groups routes from GET to POST
* api: sorting by column is no longer supported with Redis and Stub State Backends

`0.38.1`_ -- 2021-09-16
-----------------------
Changed
^^^^^^^
* catch_error:  the CatchError on_failure option can now be a Message when passed to message options

`0.38.0`_ -- 2021-09-14
-----------------------
Added
^^^^^
* actor: add the possibility of passing several additional queues to the actor, and then of choosing which one of those queue to enqueue to when sending a message.

`0.37.2`_ -- 2021-09-14
-----------------------
Fixed
^^^^^
* scheduler: add lock to scheduler sync_config function

`0.37.1`_ -- 2021-09-13
-----------------------
Added
^^^^^
* exceptions: add exception chaining

`0.37.0`_ -- 2021-09-09
-----------------------
BREAKING CHANGE
^^^^^^^^^^^^^^^
* callbacks: remove the Callbacks Middleware
* catch_error: rename option from cleanup_actor to on_failure

Changed
^^^^^^^
* catch_error: add the CatchError middleware to the default middlewares

`0.36.1`_ -- 2021-09-09
-----------------------
Changed
^^^^^^^
* broker: removed default_after and default_before and replaced them with a list that determines the middleware order

`0.36.0`_ -- 2021-09-03
-----------------------
Added
^^^^^
* max_tasks: added new middleware MaxTasks that enables stopping a worker after processing a set amount of tasks

`0.35.0`_ -- 2021-09-02
-----------------------
Added
^^^^^
* catch_error: added new middleware CatchError that enables enqueuing an actor when a message fails and won't be retried

`0.34.2`_ -- 2021-09-02
-----------------------
Fixed
^^^^^
* scheduler: Make tests use conftest fixtures

Added
^^^^^
* scheduler: added stop function that will stop the scheduler at the end of its cycle

`0.34.1`_ -- 2021-08-31
-----------------------
Changed
^^^^^^^
* results: added default_timeout argument to Result Backend to allow setting the default result timeout

`0.34.0`_ -- 2021-08-20
-----------------------
Changed
^^^^^^^
* retries: `max_retries` parameter is now also taken into account when using `retry_when` parameter

`0.33.2`_ -- 2021-08-19
-----------------------
Fixed
^^^^^^^
* retries: fix exponent computing

`0.33.1`_ -- 2021-08-19
-----------------------
Changed
^^^^^^^
* state: rename State attribute 'name' with more explicit name 'status'

`0.33.0`_ -- 2021-08-16
-----------------------
Changed
^^^^^^^
* logging_metadata: adds a new middleware which enables passing logging metadata into the message and generating this metadata by passing a callback function that returns the metadata

`0.32.0`_ -- 2021-08-13
-----------------------
Changed
^^^^^^^
* retries: adds the possibility of choosing between multiple backoff strategies

`0.31.4`_ -- 2021-08-09
-----------------------
Changed
^^^^^^^
* api: adds option route which sends the list of available options
* api: searches using the /messages/states now include the args, kwargs and options values
* api: the /actors route now sends the actors' arguments as well

`0.31.3`_ -- 2021-08-06
-----------------------
Fixed
^^^^^
* time-limit: use background thread instead of signals


`0.31.2`_ -- 2021-08-03
-----------------------
Fixed
^^^^^
* main: only log info when worker has stopped

`0.31.1`_ -- 2021-07-28
-----------------------
Fixed
^^^^^
* include messages with values evaluated as false in sort_dict return data


`0.31.0`_ -- 2021-07-26
-----------------------
Changed
^^^^^
* Middleware : The value of all broker and middleware options are now obtained with the middleware get_option_value method
* Middleware : The value returned is from higher to lower priority : message option, actor decorator option, broker option and default value.
* Middleware : It is now possible to send any option with either message, actor or broker when it makes sense


`0.30.6`_ -- 2021-07-23
-----------------------
Fixed
^^^^^
* Doc: fix doc requirements


`0.30.5`_ -- 2021-07-21
-----------------------

Changed
^^^^^^^
* Doc: add getting started

`0.30.4`_ -- 2021-07-09
-----------------------
Fixed
^^^^^
* Doc: fix all build errors

Changed
^^^^^^^
* add type hints

`0.30.3`_ -- 2021-05-20
-----------------------
Fixed
^^^^^
* Prometheus: initialise message_start_times in each thread

`0.30.2`_ -- 2021-05-20
-----------------------
Broken do not use
^^^^^^^^^^^^^^^^^
Fixed
^^^^^
* Prometheus: wrong usage of threading.local

`0.30.1`_ -- 2021-05-20
-----------------------
Broken do not use
^^^^^^^^^^^^^^^^^
Fixed
^^^^^
* Prometheus: initialize labels at worker boot and only if not None

`0.30.0`_ -- 2021-05-18
-----------------------
Broken do not use
^^^^^^^^^^^^^^^^^
Changed
^^^^^^^
* Prometheus: message_duration is now a Summary
* Prometheus: remove useless metrics
* Prometheus: actor_name label can be overridden with prometheus_label argument of actor

`0.29.1`_ -- 2021-05-14
-----------------------

Fixed
^^^^^
* State: avoid call to state_backend if state_ttl <= 0

`0.29.0`_ -- 2021-04-23
-----------------------
Added
^^^^^
* Results: add possibility to set store_results at the message level

`0.28.3`_ -- 2021-04-22
-----------------------
Fixed
^^^^^
* TimeLimit: add lock when accessing shared variable

`0.28.2`_ -- 2021-04-20
-----------------------
Fixed
^^^^^
* Typing: fix typing for actor overload and add types for broker, result and scheduler

`0.28.1`_ -- 2021-04-20
-----------------------
Fixed
^^^^^
* Typing: fix typing for group init and actor overload

`0.28.0`_ -- 2021-04-19
-----------------------
Added
^^^^^
* ResultBackend: make it more resilient, retry if save fails

`0.27.0`_ -- 2021-04-06
-----------------------
Added
^^^^^
* Middleware: add MaxMemory which stop a worker if its amount of resident memory exceed max_memory

`0.26.7`_ -- 2021-04-01
-----------------------
Added
^^^^^
* Typing: add support for typing and several type hints

`0.26.6`_ -- 2021-02-11
-----------------------
Fixed
^^^^^
* Result: catch when error cannot be serialized

`0.26.5`_ -- 2020-12-04
-----------------------
Changed
^^^^^^^
* Logging: only log a warning when a message will be retried

`0.26.4`_ -- 2020-11-26
-----------------------
Dependencies
^^^^^^^^^^^^
* update ``pytz``


`0.26.3`_ -- 2020-11-12
-----------------------
Added
^^^^^
* Worker: add exception name to ``Failed to process message`` log.


`0.26.2`_ -- 2020-11-04
-----------------------
Fix
^^^
* Priority: actor.priority is now used if priority is not present in message.options

`0.26.1`_ -- 2020-10-27
-----------------------
Changed
^^^^^^^
* Prometheus: add a registry parameter

`0.26.0`_ -- 2020-10-26
-----------------------
Changed
^^^^^^^
* |TimeLimit|: Replace SIGKILL with regular sys.exit and make this behavior disabled by default
* Rabbitmq: reconnect on connection error with exponential back-off
* Prometheus: add remoulade_worker_busy metric

`0.25.1`_ -- 2020-09-04
-----------------------
Changed
^^^^^^^
* Logs: Add truncated args and kwargs to ``Started Actor`` log.

`0.25.0`_ -- 2020-09-03
-----------------------
BREAKING CHANGE
^^^^^^^^^^^^^^^
* Remoulade now use only use one process (and remove watch feature), remove ``--proccess`` options and ``--watch`` option.

Added
^^^^^
* |TimeLimit|: send SIGKILL after delay (default: 10s) if exception fails

Changed
^^^^^^^
* Allow more recent version of ``pytz``

`0.24.0`_ -- 2020-08-19
-----------------------
BREAKING CHANGE
^^^^^^^^^^^^^^^
* allow 0 as remoulade_restart_delay env variable, which will disable consumer restart in case on connection error and return a RET_CONNECT error code (default is now 0)

`0.23.0`_ -- 2020-08-05
-----------------------
Added
^^^^^
* Class |MessageSchema| to load the data sent to enqueue a message.
* Attribute to the class |State|
   - ``group_id``
* GET methods to ``api``
   - url ``/actors``: get declared ``actors``
   - url ``/groups``: get declared ``groups``
      - schema |PageSchema|
   - url ``/messages/results/<message_id>``: get the results of a ``given message_id``
      - if the result is bigger than ``max_size`` defined in ``get_results`` return a empty string.
   - url ``/messages/requeue/<message_id>``: requeue a message asociated with a message_id
      - Requeue messages associated with a ``pipe_target`` is not support yet.
* Method ``as_dict`` to class |Actor|.
* Error |NoScheduler| raised when is tried to get an scheduler and there is not.
* Error Handler in case of |NoScheduler|
* Schema |PageSchema| to load the arguments send to ``messages/state``
* Method ``as_dict`` of |State| can receive keyword ``exclude_keys:tuple`` to exclude some keys from serialization

Changed
^^^^^^^
* ``hmset`` to ``hset`` in class |RedisResBackend| as the former is deprecated, this requires at least Redis 4.0.0 and at least redis-py 3.5.0
* Method ``api`` ``get_states`` now
    - can receive arguments defined in schema |PageSchema|
       * ``search_value``
       * ``sort_column`` a column defined in |State|, this column must be sortable
       * ``sort_direction`` possible values: ``['asc', 'desc']``, the order you want to get the register
       * ``size_page``  default ``100``: number of messages you want to retrieve
    - if ``search_value`` is defined the ``search_keys`` is a ``list`` declared in ``remoulade.api.main.py``. The current supported columns to search are ``["message_id", "name", "actor_name", "args", "kwargs"]``

Fix
^^^^^
* make ``max_size`` an argument of |StateBackend| and fix its behavior


`0.22.0`_ -- 2020-06-04
-----------------------

Added
^^^^^
* Attributes to the class |State|
   - ``actor_name(str)``
   - ``priority(int)``
   - ``enqueued_datetime(date)``
   - ``started_datetime(date)``
   - ``end_datetime(date)``
   - ``progress``
* Use of ``pipelines`` in ``get_state`` and ``set_state`` for |RedisResBackend|
* Url to ``api`` to get all scheduled jobs
   - url ``/scheduled/jobs``
* Method ``set_progress`` in Class |Message|, the progress is update using ``set_state`` of Classes type |StateBackend|
* |InvalidProgress| raised when is tried to set a progress less than 0 or greater than 1
* POST method to ``api`` to enqueue a message
   - url ``/messages``

Changed
^^^^^^^

* Signature ``asdict`` of Class |State| to ``as_dict``
* Location of ``_encoded_dict`` and ``_decoded_dict``, now is in the class |StateBackend|
* ``set`` to ``hmset`` in class |RedisResBackend|
* Behaviour of ``set_state`` of classes type ``StateBackend``. If the message_id does not exist, a new register is created, if not it updates the fields in the state, without deleting those who are not present.
* Save the datetime for states
   - **Pending**, datetime of enqueued saved in ``enqueued_datetime``
   - if **Started** datetime saved in ``started_datetime``
   - if **Failure** datetime saved in ``end_datetime``
   - if **Success** datetime saved in ``end_datetime``
* Allow to define States with `name=None`, to be able to update the `progress` without passing the name again

`0.21.0`_ -- 2020-05-07
-----------------------

Added
^^^^^
* Error |NoStateBackend| raised when is tried to access a |StateBackend| in a broker without it
* Attribute ``messaged_id`` in Class |State|
* Method ``get_states`` which returns the states storage in a |StateBackend|
* Method ``get_state_backend`` in ``broker.py``
* Module ``api`` with methods to get the state of a message by HTTP request:
   - url ``/messages/states`` returns all states in the backend
   - url ``/messages/states?name=NameState`` returns all states in the backend whose state is equal to NameState, this should be defined in StateNamesEnum
   - url ``/messages/state/message_id`` return the state of a given ``message_id``
* Class |TestMessageStateAPI| responsible to test the API of |StateBackend|
* Add **Flask** as an extra dependency
* Add |CurrentMessage| Middleware that exposes the current message via a thread local variable, useful to access the message within the actor
* Add |CurrentMessage| to the list of ``default_middleware``
* Add new POST method ``cancel_message`` in module ``api``
   - url ``/messages/cancel/message_id``

`0.20.0`_ -- 2020-04-07
-----------------------
BREAKING CHANGE
^^^^^^^^^^^^^^^
* reduce: now take a `size` argument and a `merge_kwargs` argument. `size` determine the number of message that are taken
  at each reduce (merge) step and the `merge_kwargs` are the attributes that will be passed to the merge messages.

Added
^^^^^
* StateNamesEnum (type :**Enum**) contains the possible states that can have a message:
   - **Started** a |Message| that  has not been processed
   - **Pending** a |Message| that has been enqueued
   - **Skipped** a |Message| that has been skipped
   - **Canceled** a |Message| that has been cancelled
   - **Failure** a |Message| that has been processed and raise an **Exception**
   - **Success** a |Message| that has been processed and does not raise an **Exception**
* Class |State| represents the current state of a message, the state is defined by:
   - **StateNamesEnum.name** the name of the state
   - args The arguments of the message, they are storage if they are less than  MessageState.max_size
   - kwargs The keyword arguments of the message, they are storage if they are less than  MessageState.max_size
* Middleware |MessageState| used to update the state of a message in a Backend, the constructor receives
   - backend (type : |StateBackend|)
   - state_ttl
   - max_size
* Abstract Backend |StateBackend| with methods |set_state| and |get_state| to set and get a |State| from the Backend
* |RedisResBackend| and |StubBackend| (type :|StateBackend|)
* |InvalidStateError| raised when is tried to create an Invalid State

`0.19.0`_ -- 2019-01-31
-----------------------
BREAKING CHANGE
^^^^^^^^^^^^^^^
* result: when passing raise_on_error=False to a function to get a result (message, group, backend), the returned object in case of error is an instance of ErrorStored instead of the FailureResult singleton value.


`0.18.3`_ -- 2019-01-29
-----------------------
Fix
^^^^^
* redis: result ttl was set to null when get_result was called with block=True and forget=True

`0.18.2`_ -- 2019-01-24
-----------------------
Fix
^^^^^
* build: relax version limit on prometheus_client

`0.18.1`_ -- 2019-12-30
-----------------------
Fix
^^^^^
* generic: Fix error when abstract=False

`0.18.0`_ -- 2019-12-05
-----------------------
Added
^^^^^
* generic: allow Meta inheritance

`0.17.0`_ -- 2019-08-22
-----------------------
Added
^^^^^
* Rabbitmq: add dead_queue_max_length

Fix
^^^
* Channel pool: use LIFO queue instead of FIFO queue

`0.16.2`_ -- 2019-08-01
-----------------------

Fix
^^^
* Pipeline: build should be consistent

`0.16.1`_ -- 2019-07-11
-----------------------

Fix
^^^
* Scheduler: put scheduler outside of main module

`0.16.0`_ -- 2019-06-28
-----------------------

Added
^^^^^
* Channel pool: prevent the opening of one channel per thread (`#91`_)

`0.15.0`_ -- 2019-05-24
-----------------------

Added
^^^^^
* Remoulade scheduler (`#86`_)

`0.14.0`_ -- 2018-04-09
-----------------------

Changed
^^^^^^^
* Use (thread safe) amqpstorm_ instead of pika (`#77`_)

Added
^^^^^
* Raise error when starting worker with LocalBroker (`#84`_)


`0.13.0`_ -- 2018-03-20
-----------------------

Added
^^^^^
* Add log on start/end of group completion

Fix
^^^
* Store group message_ids in backend (`#79`_)

`0.12.0`_ -- 2018-03-14
-----------------------

Added
^^^^^
* Log message args and kwargs in extra field when error

Changed
^^^^^^^
* Local broker do not declare its middleware anymore

`0.11.0`_ -- 2018-03-08
-----------------------

Remove
^^^^^^

* Cancelable option in |Cancel| and as actor option.

`0.10.0`_ -- 2018-02-28
-----------------------

BREAKING CHANGE
^^^^^^^^^^^^^^^
* higher priorities are now processed before to be consistent with rabbimq

Added
^^^^^
* Priority support in RabbitMQ broker
* cancel method on |group| and |pipeline|
* declare_actors helper function, which take a list of actors and declare it to the current broker

Remove
^^^^^^

* ResultNotStored error (fix bug when store_results is set at middleware level)

`0.9.0`_ -- 2018-01-18
----------------------

Added
^^^^^

* |Cancel| middleware, and a |message_cancel| method to prevent processing of messages which have been enqueued
* |cancel_on_error| which cancel all group members on member failure.

`0.8.2`_ -- 2018-12-31
----------------------

Fixed
^^^^^

* Workers wait for RMQ messages to be acked upon shutdown.
* Pipelines no longer continue when a message is failed.


`0.8.1`_ -- 2018-12-17
----------------------

Fixed
^^^^^

* Declare queues on each ConnectionError even if the queue has already been declare (before a worker restart was needed if a queue was deleted)
* RedisBackend.get_result saving a ForgottenResult every time

`0.8.0`_ -- 2018-12-07
----------------------

Added
^^^^^

* Added Result |completed| without needing to get the full result, also true with |completed_count|.

Fixed
^^^^^

* Failure of children of pipeline when a message fail (in the case of pipeline of groups)

`0.7.0`_ -- 2018-12-04
----------------------

Added
^^^^^

* Support for |pipeline| with groups (result backend necessary for this)
* Added group_id to |group|

Changed
^^^^^^^
* Remove support for group of groups, a |group| take as input a |pipeline| or a message
* |get_result_backend| now raise NoResultBackend if there is no |ResultBackend|
* Merged PipelineResult and GroupResult into |CollectionResult|
* |message_get_result| on forgotten results now returns None
* Update redis-py to 3.0.1


`0.6.0`_ -- 2018-11-23
----------------------

Fixed
^^^^^

* Better handling of RabbitMQ queue declaration (consumer will keep trying on ConnectionError)

Changed
^^^^^^^

* Prevent access to |Message| result if the linked actor do not have store_results=True

Added
^^^^^

* Add Prefetch multiplier to cli parameters

`0.5.0`_ -- 2018-11-15
----------------------

Breaking Changes
^^^^^^^^^^^^^^^^
* Added property result to |Message| (type: |Result|), and |pipeline| (type: PipelineResult) and results to |group| (type: GroupResults). These new Class get the all result linked logic (get instead of get_result)
* Rename MessageResult to |Result|
* Removed get_results from |Message|, |group| and |pipeline| (and all results related method like completed_count, ...). Use the new result property for |Message| and |pipeline|, and results for |group|.

`0.4.0`_ -- 2018-11-15
----------------------

Changed
^^^^^^^

* Rename FAILURE_RESULT to **FailureResult** (for consistency)

Added
^^^^^

* Add MessageResult which can be created from a message_id and can be used to retrieved the result of the linked message

Fixed
^^^^^

* Clear timer on before_process_stop

`0.3.0`_ -- 2018-11-12
----------------------

Changed
^^^^^^^

* |message_get_result| has a forget parameter, if True, the result will be deleted from the result backend when retrieved
* Remove support for memcached
* Log an error when an exception is raised while processing a message (previously it was a warning)


`0.2.0`_ -- 2018-11-09
----------------------

Changed
^^^^^^^

* |Results| now stores errors as well as results and will raise an |ErrorStored| the actor fail
* |message_get_result| has a raise_on_error parameter, True by default. If False, the method return **FailureResult** if there is no Error else raise an |ErrorStored|.
* |Middleware| have a ``default_before`` and  ``default_after`` to place them by default in the middleware list
* |Results| needs to be before |Retries|
* **Prometheus** removed from default middleware

`0.1.0`_ -- 2018-10-24
----------------------

Added
^^^^^

* A |LocalBroker| equivalent to CELERY_ALWAYS_EAGER.

Changed
^^^^^^^

* Name of project to Remoulade (fork of Dramatiq v1.3.0)
* Delete URLRabbitmqBroker
* Delete RedisBroker
* Set default max_retries to 0
* Declare RabbitMQ Queue on first message enqueuing

Fixed
^^^^^

* pipe_ignore was not recovered from right message

.. _amqpstorm: https://www.amqpstorm.io/

.. _#91: https://github.com/wiremind/remoulade/pull/91
.. _#86: https://github.com/wiremind/remoulade/pull/86
.. _#79: https://github.com/wiremind/remoulade/issues/79
.. _#84: https://github.com/wiremind/remoulade/issues/84
.. _#77: https://github.com/wiremind/remoulade/issues/77

.. _0.42.3: https://github.com/wiremind/remoulade/releases/tag/v0.42.3
.. _0.42.2: https://github.com/wiremind/remoulade/releases/tag/v0.42.2
.. _0.42.1: https://github.com/wiremind/remoulade/releases/tag/v0.42.1
.. _0.42.0: https://github.com/wiremind/remoulade/releases/tag/v0.42.0
.. _0.41.1: https://github.com/wiremind/remoulade/releases/tag/v0.41.1
.. _0.41.0: https://github.com/wiremind/remoulade/releases/tag/v0.41.0
.. _0.40.2: https://github.com/wiremind/remoulade/releases/tag/v0.40.2
.. _0.40.1: https://github.com/wiremind/remoulade/releases/tag/v0.40.1
.. _0.40.0: https://github.com/wiremind/remoulade/releases/tag/v0.40.0
.. _0.39.3: https://github.com/wiremind/remoulade/releases/tag/v0.39.3
.. _0.39.2: https://github.com/wiremind/remoulade/releases/tag/v0.39.2
.. _0.39.1: https://github.com/wiremind/remoulade/releases/tag/v0.39.1
.. _0.39.0: https://github.com/wiremind/remoulade/releases/tag/v0.39.0
.. _0.38.1: https://github.com/wiremind/remoulade/releases/tag/v0.38.1
.. _0.38.0: https://github.com/wiremind/remoulade/releases/tag/v0.38.0
.. _0.37.2: https://github.com/wiremind/remoulade/releases/tag/v0.37.2
.. _0.37.1: https://github.com/wiremind/remoulade/releases/tag/v0.37.1
.. _0.37.0: https://github.com/wiremind/remoulade/releases/tag/v0.37.0
.. _0.36.1: https://github.com/wiremind/remoulade/releases/tag/v0.36.1
.. _0.36.0: https://github.com/wiremind/remoulade/releases/tag/v0.36.0
.. _0.35.0: https://github.com/wiremind/remoulade/releases/tag/v0.35.0
.. _0.34.2: https://github.com/wiremind/remoulade/releases/tag/v0.34.2
.. _0.34.1: https://github.com/wiremind/remoulade/releases/tag/v0.34.1
.. _0.34.0: https://github.com/wiremind/remoulade/releases/tag/v0.34.0
.. _0.33.2: https://github.com/wiremind/remoulade/releases/tag/v0.33.2
.. _0.33.1: https://github.com/wiremind/remoulade/releases/tag/v0.33.1
.. _0.33.0: https://github.com/wiremind/remoulade/releases/tag/v0.33.0
.. _0.32.0: https://github.com/wiremind/remoulade/releases/tag/v0.32.0
.. _0.31.4: https://github.com/wiremind/remoulade/releases/tag/v0.31.4
.. _0.31.3: https://github.com/wiremind/remoulade/releases/tag/v0.31.3
.. _0.31.2: https://github.com/wiremind/remoulade/releases/tag/v0.31.2
.. _0.31.1: https://github.com/wiremind/remoulade/releases/tag/v0.31.1
.. _0.31.0: https://github.com/wiremind/remoulade/releases/tag/v0.31.0
.. _0.30.6: https://github.com/wiremind/remoulade/releases/tag/v0.30.6
.. _0.30.5: https://github.com/wiremind/remoulade/releases/tag/v0.30.5
.. _0.30.4: https://github.com/wiremind/remoulade/releases/tag/v0.30.4
.. _0.30.3: https://github.com/wiremind/remoulade/releases/tag/v0.30.3
.. _0.30.2: https://github.com/wiremind/remoulade/releases/tag/v0.30.2
.. _0.30.1: https://github.com/wiremind/remoulade/releases/tag/v0.30.1
.. _0.30.0: https://github.com/wiremind/remoulade/releases/tag/v0.30.0
.. _0.29.1: https://github.com/wiremind/remoulade/releases/tag/v0.29.1
.. _0.29.0: https://github.com/wiremind/remoulade/releases/tag/v0.29.0
.. _0.28.3: https://github.com/wiremind/remoulade/releases/tag/v0.28.3
.. _0.28.2: https://github.com/wiremind/remoulade/releases/tag/v0.28.2
.. _0.28.1: https://github.com/wiremind/remoulade/releases/tag/v0.28.1
.. _0.28.0: https://github.com/wiremind/remoulade/releases/tag/v0.28.0
.. _0.27.0: https://github.com/wiremind/remoulade/releases/tag/v0.27.0
.. _0.26.7: https://github.com/wiremind/remoulade/releases/tag/v0.26.7
.. _0.26.6: https://github.com/wiremind/remoulade/releases/tag/v0.26.6
.. _0.26.5: https://github.com/wiremind/remoulade/releases/tag/v0.26.5
.. _0.26.4: https://github.com/wiremind/remoulade/releases/tag/v0.26.4
.. _0.26.3: https://github.com/wiremind/remoulade/releases/tag/v0.26.3
.. _0.26.2: https://github.com/wiremind/remoulade/releases/tag/v0.26.2
.. _0.26.1: https://github.com/wiremind/remoulade/releases/tag/v0.26.1
.. _0.26.0: https://github.com/wiremind/remoulade/releases/tag/v0.26.0
.. _0.25.1: https://github.com/wiremind/remoulade/releases/tag/v0.25.1
.. _0.25.0: https://github.com/wiremind/remoulade/releases/tag/v0.25.0
.. _0.24.0: https://github.com/wiremind/remoulade/releases/tag/v0.24.0
.. _0.23.0: https://github.com/wiremind/remoulade/releases/tag/v0.23.0
.. _0.22.0: https://github.com/wiremind/remoulade/releases/tag/v0.22.0
.. _0.21.0: https://github.com/wiremind/remoulade/releases/tag/v0.21.0
.. _0.20.0: https://github.com/wiremind/remoulade/releases/tag/v0.20.0
.. _0.19.0: https://github.com/wiremind/remoulade/releases/tag/v0.19.0
.. _0.18.3: https://github.com/wiremind/remoulade/releases/tag/v0.18.3
.. _0.18.2: https://github.com/wiremind/remoulade/releases/tag/v0.18.2
.. _0.18.1: https://github.com/wiremind/remoulade/releases/tag/v0.18.1
.. _0.18.0: https://github.com/wiremind/remoulade/releases/tag/v0.18.0
.. _0.17.0: https://github.com/wiremind/remoulade/releases/tag/v0.17.0
.. _0.16.2: https://github.com/wiremind/remoulade/releases/tag/v0.16.2
.. _0.16.1: https://github.com/wiremind/remoulade/releases/tag/v0.16.1
.. _0.16.0: https://github.com/wiremind/remoulade/releases/tag/v0.16.0
.. _0.15.0: https://github.com/wiremind/remoulade/releases/tag/v0.15.0
.. _0.14.0: https://github.com/wiremind/remoulade/releases/tag/v0.14.0
.. _0.13.0: https://github.com/wiremind/remoulade/releases/tag/v0.13.0
.. _0.12.0: https://github.com/wiremind/remoulade/releases/tag/v0.12.0
.. _0.11.0: https://github.com/wiremind/remoulade/releases/tag/v0.11.0
.. _0.10.0: https://github.com/wiremind/remoulade/releases/tag/v0.10.0
.. _0.9.0: https://github.com/wiremind/remoulade/releases/tag/v0.9.0
.. _0.8.2: https://github.com/wiremind/remoulade/releases/tag/v0.8.2
.. _0.8.1: https://github.com/wiremind/remoulade/releases/tag/v0.8.1
.. _0.8.0: https://github.com/wiremind/remoulade/releases/tag/v0.8.0
.. _0.7.0: https://github.com/wiremind/remoulade/releases/tag/v0.7.0
.. _0.6.0: https://github.com/wiremind/remoulade/releases/tag/v0.6.0
.. _0.5.0: https://github.com/wiremind/remoulade/releases/tag/v0.5.0
.. _0.4.0: https://github.com/wiremind/remoulade/releases/tag/v0.4.0
.. _0.3.0: https://github.com/wiremind/remoulade/releases/tag/v0.3.0
.. _0.2.0: https://github.com/wiremind/remoulade/releases/tag/v0.2.0
.. _0.1.0: https://github.com/wiremind/remoulade/releases/tag/v0.1.0
