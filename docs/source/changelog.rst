.. include:: global.rst

Changelog
=========

All notable changes to this project will be documented in this file.

`Unreleased`_
-------------

`0.22.0`_ -- 2020-06-04
-------------------------

Added
^^^^^
* Attributes to the class |State|
   - ``actor_name(str)``
   - ``priority(int)``
   - ``enqueued_datetime(date)``
   - ``started_datetime(date)``
   - ``end_datetime(date)``
   - ``progress``
* Use of ``pipelines`` in ``get_state`` and ``set_state`` for |RedisBackend|
* Url to ``api`` to get all scheduled jobs
   - url ``/scheduled/jobs``
* Method ``set_progress`` in Class |Message|, the progress is update using ``set_state`` of Classes type |StateBackend|
* |InvalidProgress| raised when is tried to set a progress less than 0 or greater than 1
* POST method to ``api`` to enqueue a message
   - url ``/messages``
* Class |MessageSchema| to load the data sent to enqueue a message.

Changed
^^^^^^^

* Signature ``asdict`` of Class |State| to ``as_dict``
* Location of ``_encoded_dict`` and ``_decoded_dict``, now is in the class |StateBackend|
* ``set`` to ``hmset`` in class |RedisBackend|
* Behaviour of ``set_state`` of classes type ``StateBackend``. If the message_id does not exist, a new register is created, if not it updates the fields in the state, without deleting those who are not present.
* Save the datetime for states
   - |Pending|, datetime of enqueued saved in ``enqueued_datetime``
   - if |Started| datetime saved in ``started_datetime``
   - if |Failure| datetime saved in ``end_datetime``
   - if |Success| datetime saved in ``end_datetime``
* Allow to define States with `name=None`, to be able to update the `progress` without passing the name again

`0.21.0`_ -- 2020-05-07
-------------------------

Added
^^^^^
* Error |NoStateBackend| raised when is tried to access a |StateBackend| in a broker without it
* Attribute ``messaged_id`` in Class |State|
* Method ``get_states`` which returns the states storage in a |StateBackend|
* Method ``get_state_backend`` in ``broker.py``
* Module ``api`` with methods to get the state of a message by HTTP request:
   - url ``/messages/states`` returns all states in the backend
   - url ``/messages/states?name=NameState`` returns all states in the backend whose state is equal to NameState, this should be defined in |StateNamesEnum|
   - url ``/messages/state/message_id`` return the state of a given ``message_id``
* Class |TestMessageStateAPI| responsible to test the API of |StateBackend|
* Add |Flask| as an extra dependency
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
* |StateNamesEnum| (type :|Enum|) contains the possible states that can have a message:
   - |Started| a |Message| that  has not been processed
   - |Pending| a |Message| that has been enqueued
   - |Skipped| a |Message| that has been skipped
   - |Canceled| a |Message| that has been cancelled
   - |Failure| a |Message| that has been processed and raise an |Exception|
   - |Success| a |Message| that has been processed and does not raise an |Exception|
* Class |State| represents the current state of a message, the state is defined by:
   - |StateNamesEnum.name| the name of the state
   - args The arguments of the message, they are storage if they are less than  MessageState.max_size
   - kwargs The keyword arguments of the message, they are storage if they are less than  MessageState.max_size
* Middleware |MessageState| used to update the state of a message in a Backend, the constructor receives
   - backend (type : |StateBackend|)
   - state_ttl
   - max_size
* Abstract Backend |StateBackend| with methods |set_state| and |get_state| to set and get a |State| from the Backend
* |RedisBackend| and |StubBackend| (type :|StateBackend|)
* |InvalidStateError| raised when is tried to create an Invalid State
* |TestMessageState| tests of the Middleware |MessageState|
* |TestState| tests of the class |State|

`0.19.0`_ -- 2019-01-31
-----------------------
BREAKING CHANGE
^^^^^^^^^^^^^^^
* result: when passing raise_on_error=False to a function to get a result (message, group, backend), the returned
object in case of error is an instance of ErrorStored instead of the FailureResult singleton value.


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
* Rabbimq: add dead_queue_max_length

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
----------------------

Added
^^^^^
* Channel pool: prevent the opening of one channel per thread (`#86`_)
.. _#86: https://github.com/wiremind/remoulade/pull/91


`0.15.0`_ -- 2019-05-24
----------------------

Added
^^^^^
* Remoulade scheduler (`#86`_)
.. _#86: https://github.com/wiremind/remoulade/pull/86


`0.14.0`_ -- 2018-04-09
----------------------

Changed
^^^^^^^
* Use (thread safe) amqpstorm_ instead of pika (`#77`_)
.. _#77: https://github.com/wiremind/remoulade/issues/77
.. _amqpstorm: https://www.amqpstorm.io/

Added
^^^^^
* Raise error when starting worker with LocalBroker (`#84`_)
.. _#84: https://github.com/wiremind/remoulade/issues/84


`0.13.0`_ -- 2018-03-20
----------------------

Added
^^^^^
* Add log on start/end of group completion

Fix
^^^
* Store group message_ids in backend (`#79`_)

.. _#79: https://github.com/wiremind/remoulade/issues/79

`0.12.0`_ -- 2018-03-14
----------------------

Added
^^^^^
* Log message args and kwargs in extra field when error

Changed
^^^^^^^
* Local broker do not declare its middleware anymore

`0.11.0`_ -- 2018-03-08
----------------------

Remove
^^^^^^

* Cancelable option in |Cancel| and as actor option.

`0.10.0`_ -- 2018-02-28
----------------------

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

* Declare queues on each ConnectionError even if the queue has already been declare (before a worker restart was
needed if a queue was deleted)
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
* Added property result to |Message| (type: |Result|), and |pipeline| (type: PipelineResult) and results to |group|
 (type: GroupResults). These new Class get the all result linked logic (get instead of get_result)
* Rename MessageResult to |Result|
* Removed get_results from |Message|, |group| and |pipeline| (and all results related methods
like completed_count, ...). Use the new result property for |Message| and |pipeline|, and results for |group|.

`0.4.0`_ -- 2018-11-15
----------------------

Changed
^^^^^^^

* Rename FAILURE_RESULT to |FailureResult| (for consistency)

Added
^^^^^

* Add MessageResult which can be created from a message_id and can be used to retrieved the result of the linked
message

Fixed
^^^^^

* Clear timer on before_process_stop

`0.3.0`_ -- 2018-11-12
----------------------

Changed
^^^^^^^

* |message_get_result| has a forget parameter, if True, the result will be deleted from the result backend when
retrieved
* Remove support for memcached
* Log an error when an exception is raised while processing a message (previously it was a warning)


`0.2.0`_ -- 2018-11-09
----------------------

Changed
^^^^^^^

* |Results| now stores errors as well as results and will raise an |ErrorStored| the actor fail
* |message_get_result| has a raise_on_error parameter, True by default. If False, the method return |FailureResult| if
there is no Error else raise an |ErrorStored|.
* |Middleware| have a ``default_before`` and  ``default_after`` to place them by default in the middleware list
* |Results| needs to be before |Retries|
* |Promotheus| removed from default middleware

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

.. _Unreleased: https://github.com/wiremind/remoulade/compare/v0.20.0...HEAD
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
