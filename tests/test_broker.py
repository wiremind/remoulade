import platform

import pytest

import remoulade
import remoulade.broker
from remoulade.brokers.rabbitmq import RabbitmqBroker
from remoulade.brokers.stub import StubBroker
from remoulade.middleware import AgeLimit, Middleware, ShutdownNotifications, TimeLimit

CURRENT_OS = platform.system()
skip_on_windows = pytest.mark.skipif(CURRENT_OS == "Windows", reason="test skipped on Windows")


class EmptyMiddleware(Middleware):
    pass


def test_broker_uses_rabbitmq_if_not_set():
    # Given that no global broker is set
    remoulade.broker.global_broker = None

    # If I try to get the global broker
    # I expect a ValueError to be raised
    with pytest.raises(ValueError) as e:
        remoulade.get_broker()

    assert str(e.value) == "Broker not found, are you sure you called set_broker(broker) ?"


def test_change_broker(stub_broker):
    # Given that some actors
    @remoulade.actor
    def add(x, y):
        return x + y

    @remoulade.actor
    def modulo(x, y):
        return x % y

    # And these actors are declared
    stub_broker.declare_actor(add)
    stub_broker.declare_actor(modulo)

    # Given a new broker
    new_broker = StubBroker()

    remoulade.change_broker(new_broker)

    # I expect them to have the same actors
    assert stub_broker.actors == new_broker.actors


def test_declare_actors(stub_broker):
    # Given that some actors
    @remoulade.actor
    def add(x, y):
        return x + y

    @remoulade.actor
    def modulo(x, y):
        return x % y

    actors = [add, modulo]
    remoulade.declare_actors(actors)

    assert set(stub_broker.actors.values()) == set(actors)


def test_declare_actors_no_broker():
    remoulade.broker.global_broker = None
    with pytest.raises(ValueError):
        remoulade.declare_actors([])


def test_middleware_is_inserted_correctly():
    broker = StubBroker(middleware=[])

    assert len(broker.middleware) == 0

    broker.add_middleware(AgeLimit())

    assert len(broker.middleware) == 1

    broker.add_middleware(ShutdownNotifications())

    assert len(broker.middleware) == 2

    broker.add_middleware(TimeLimit())

    assert len(broker.middleware) == 3
    assert isinstance(broker.middleware[1], TimeLimit)


def test_custom_middleware_is_appended(stub_broker):
    class TestMiddleware(Middleware):
        pass

    middleware_count = len(stub_broker.middleware)

    stub_broker.add_middleware(TestMiddleware())

    assert len(stub_broker.middleware) == middleware_count + 1
    assert isinstance(stub_broker.middleware[middleware_count], TestMiddleware)


def test_can_instantiate_brokers_without_middleware():
    # Given that I have an empty list of middleware
    # When I pass that to the RMQ Broker
    broker = RabbitmqBroker(middleware=[])

    # Then I should get back a broker with not middleware
    assert not broker.middleware


def test_duplicate_middleware(stub_broker):
    # Given that I have a broker with the default middleware
    middleware_count = len(stub_broker.middleware)

    # When I add a middleware that is already added with options
    stub_broker.add_middleware(AgeLimit(max_age=2))

    # Then I should have the same amount of middleware, and the Age Limit middleware should be the new one
    assert len(stub_broker.middleware) == middleware_count
    assert stub_broker.get_middleware(AgeLimit).max_age == 2


def test_remove_middleware(stub_broker):
    # Given that I have a broker with multiple middlewares
    stub_broker.add_middleware(AgeLimit())
    stub_broker.add_middleware(ShutdownNotifications())
    stub_broker.add_middleware(TimeLimit())
    middleware_count = len(stub_broker.middleware)

    # When I add a middleware that is already added with options
    stub_broker.remove_middleware(ShutdownNotifications)

    # Then I should have the right amount of middleware, and the ShutdownNotifications middleware should not be there
    assert len(stub_broker.middleware) == middleware_count - 1
    assert stub_broker.get_middleware(ShutdownNotifications) is None
