.. References

.. |Brokers| replace:: :class:`Brokers<remoulade.Broker>`
.. |Broker| replace:: :class:`Broker<remoulade.Broker>`
.. |Cancel| replace:: :class:`Cancel<remoulade.cancel.Cancel>`
.. |CatchError| replace:: :class:`CatchError<remoulade.middleware.CatchError>`
.. |CollectionResults|  replace:: :class:`CollectionResults<remoulade.collection_results>`
.. |CollectionResult| replace:: :class:`CollectionResult<remoulade.CollectionResult>`
.. |CurrentMessage| replace:: :class:`CurrentMessage<remoulade.middleware.CurrentMessage>`
.. |Encoders| replace:: :class:`Encoders<remoulade.Encoder>`
.. |ErrorStored| replace:: :class:`ErrorStored<remoulade.results.errors.ErrorStored>`
.. |GenericActors| replace:: :class:`class-based actors<remoulade.GenericActor>`
.. |GroupResults|  replace:: :class:`GroupResults<remoulade.composition_result>`
.. |Groups| replace:: :func:`Groups<remoulade.group>`
.. |Interrupt| replace:: :class:`Interrupt<remoulade.middleware.Interrupt>`
.. |InvalidProgress| replace:: :class:`InvalidProgress<remoulade.errors.InvalidProgress>`
.. |InvalidStateError| replace:: :class:`InvalidStateError<remoulade.errors.InvalidStateError>`
.. |LocalBroker| replace:: :class:`LocalBroker<remoulade.brokers.local.LocalBroker>`
.. |LoggingMetadata| replace:: :class:`LoggingMetadata<remoulade.middleware.LoggingMetadata>`
.. |MessageSchema| replace:: :class:`MessageSchema<remoulade.api.schema.MessageSchema>`
.. |MessageState| replace:: :class:`MessageState<remoulade.state.MessageState>`
.. |Messages| replace:: :class:`Messages<remoulade.Message>`
.. |Message| replace:: :class:`Message<remoulade.Message>`
.. |MiddlewareError| replace:: :class:`MiddlewareError<remoulade.middleware.MiddlewareError>`
.. |Middleware| replace:: :class:`Middleware<remoulade.Middleware>`
.. |NoScheduler| replace:: :class:`NoScheduler<remoulade.errors.NoScheduler>`
.. |NoStateBackend| replace:: :class:`NoStateBackend<remoulade.errors.NoStateBackend>`
.. |PageSchema| replace:: :class:`PageSchema<remoulade.api.schema.PageSchema>`
.. |Prometheus| replace:: :class:`Prometheus<remoulade.middleware.Prometheus>`
.. |RabbitmqBroker_join| replace:: :meth:`join<remoulade.brokers.rabbitmq.RabbitmqBroker.join>`
.. |RabbitmqBroker| replace:: :class:`RabbitmqBroker<remoulade.brokers.rabbitmq.RabbitmqBroker>`
.. |RateLimitExceeded| replace:: :class:`RateLimitExceeded<remoulade.RateLimitExceeded>`
.. |RateLimiters| replace:: :class:`RateLimiters<remoulade.rate_limits.RateLimiter>`
.. |RedisRLBackend| replace:: :class:`Redis<remoulade.rate_limits.backends.RedisBackend>`
.. |RedisResBackend| replace:: :class:`Redis<remoulade.results.backends.RedisBackend>`
.. |RemouladeError| replace:: :class:`RemouladeError<remoulade.RemouladeError>`
.. |ResultBackends| replace:: :class:`ResultBackends<remoulade.results.ResultBackend>`
.. |ResultBackend| replace:: :class:`ResultBackend<remoulade.results.ResultBackend>`
.. |ResultMissing| replace:: :class:`ResultMissing<remoulade.results.ResultMissing>`
.. |ResultTimeout| replace:: :class:`ResultTimeout<remoulade.results.ResultTimeout>`
.. |Results| replace:: :class:`Results<remoulade.results.Results>`
.. |Result| replace:: :class:`Result<remoulade.Result>`
.. |Retries| replace:: :class:`Retries<remoulade.middleware.Retries>`
.. |ShutdownNotifications| replace:: :class:`ShutdownNotifications<remoulade.middleware.ShutdownNotifications>`
.. |Shutdown| replace:: :class:`Shutdown<remoulade.middleware.Shutdown>`
.. |SkipMessage| replace:: :class:`SkipMessage<remoulade.middleware.SkipMessage>`
.. |StateBackend| replace:: :class:`StateBackend<remoulade.state.StateBackend>`
.. |StateStatusesEnum| replace:: :class:`StateStatusesEnum<remoulade.state.StateStatusesEnum>`
.. |State| replace:: :class:`State<remoulade.state.State>`
.. |StubBackend| replace:: :class:`StubBackend<remoulade.results.backend.StubBackend>`
.. |StubBroker_flush_all| replace:: :meth:`StubBroker.flush_all<remoulade.brokers.stub.StubBroker.flush_all>`
.. |StubBroker_flush| replace:: :meth:`StubBroker.flush<remoulade.brokers.stub.StubBroker.flush>`
.. |StubBroker_join| replace:: :meth:`StubBroker.join<remoulade.brokers.stub.StubBroker.join>`
.. |StubBroker| replace:: :class:`StubBroker<remoulade.brokers.stub.StubBroker>`
.. |TestMessageStateAPI| replace:: :class:`TestMessageStateAPI<remoulade.tests.state.TestMessageStateAPI>`
.. |TimeLimitExceeded| replace:: :class:`TimeLimitExceeded<remoulade.middleware.TimeLimitExceeded>`
.. |TimeLimit| replace:: :class:`TimeLimit<remoulade.middleware.TimeLimit>`
.. |WindowRateLimiter| replace:: :class:`WindowRateLimiter<remoulade.rate_limits.WindowRateLimiter>`
.. |Worker_join| replace:: :meth:`Worker.join<remoulade.Worker.join>`
.. |Worker_pause| replace:: :meth:`Worker.pause<remoulade.Worker.pause>`
.. |Worker_resume| replace:: :meth:`Worker.resume<remoulade.Worker.resume>`
.. |Worker| replace:: :meth:`Worker<remoulade.Worker>`
.. |actor| replace:: :func:`actor<remoulade.actor>`
.. |add_middleware| replace:: :meth:`add_middleware<remoulade.Broker.add_middleware>`
.. |after_skip_message| replace:: :meth:`after_skip_message<remoulade.Middleware.after_skip_message>`
.. |before_consumer_thread_shutdown| replace:: :meth:`before_consumer_thread_shutdown<remoulade.Middleware.before_consumer_thread_shutdown>`
.. |before_worker_thread_shutdown| replace:: :meth:`before_worker_thread_shutdown<remoulade.Middleware.before_worker_thread_shutdown>`
.. |cancel_on_error| replace:: :meth:`cancel_on_error<remoulade.group.cancel_on_error>`
.. |completed_count| replace:: :meth:`completed_count<remoulade.CollectionResults.completed_count>`
.. |completed| replace:: :meth:`completed<remoulade.Result.completed>`
.. |get_result_backend|  replace:: :meth:`get_result_backend<remoulade.Broker.get_result_backend>`
.. |get_state| replace:: :meth:`get_state<remoulade.state.get_state>`
.. |group| replace:: :func:`group<remoulade.group>`
.. |message_cancel| replace:: :meth:`cancel<remoulade.message.cancel>`
.. |message_get_result| replace:: :meth:`get_result<remoulade.message.get_result>`
.. |pipeline_result_get| replace:: :meth:`get<remoulade.CollectionResults.get>`
.. |pipeline_results_get| replace:: :meth:`get<remoulade.CollectionResults.get>`
.. |pipeline| replace:: :func:`pipeline<remoulade.pipeline>`
.. |rate_limits| replace:: :mod:`remoulade.rate_limits`
.. |remoulade| replace:: :mod:`remoulade`
.. |send_with_options| replace:: :meth:`send_with_options<remoulade.Actor.send_with_options>`
.. |send| replace:: :meth:`send<remoulade.Actor.send>`
.. |set_state| replace:: :meth:`set_state<remoulade.state.set_state>`

.. _gevent: http://www.gevent.org/
.. _RabbitMQ: https://www.rabbitmq.com
.. _Redis: https://redis.io
.. _PostgreSQL: https://www.postgresql.org
.. _Dramatiq: https://dramatiq.io
