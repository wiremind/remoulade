import pytest

from remoulade.brokers.rabbitmq import ChannelPool
from remoulade.errors import ChannelPoolTimeout


def test_channel_pool_acquire(mock_channel_factory):
    channel_pool = ChannelPool(channel_factory=mock_channel_factory, pool_size=10)

    with channel_pool.acquire() as channel_1:
        assert channel_1.id == 0
        assert len(channel_pool) == 9

        with channel_pool.acquire() as channel_2:
            assert channel_2.id == 1
            assert len(channel_pool) == 8

    assert len(channel_pool) == 10
    items_in_pool = []
    while len(channel_pool):
        channel = channel_pool.get()
        if channel is not None:
            items_in_pool.append(channel.id)
        else:
            items_in_pool.append(channel)

    assert items_in_pool == [0, 1, None, None, None, None, None, None, None, None]


def test_channel_pool_acquire_one_used(mock_channel_factory):
    channel_pool = ChannelPool(channel_factory=mock_channel_factory, pool_size=10)

    for _ in range(10):
        with channel_pool.acquire() as channel_1:
            assert channel_1.id == 0
            assert len(channel_pool) == 9

    assert len(channel_pool) == 10
    items_in_pool = []
    while len(channel_pool):
        channel = channel_pool.get()
        if channel is not None:
            items_in_pool.append(channel.id)
        else:
            items_in_pool.append(channel)

    assert items_in_pool == [0, None, None, None, None, None, None, None, None, None]


def test_raise_channel_pool_timeout(mock_channel_factory):
    channel_pool = ChannelPool(channel_factory=mock_channel_factory, pool_size=1)
    with channel_pool.acquire():
        assert len(channel_pool) == 0
        with pytest.raises(ChannelPoolTimeout):
            with channel_pool.acquire(timeout=1):
                pass


def test_channel_pool_clear(mock_channel_factory):
    channel_pool = ChannelPool(channel_factory=mock_channel_factory, pool_size=5)
    with channel_pool.acquire():
        with channel_pool.acquire():
            with channel_pool.acquire():
                with channel_pool.acquire():
                    with channel_pool.acquire():
                        pass

    channel_pool.clear()
    items_in_pool = []
    while len(channel_pool):
        items_in_pool.append(channel_pool.get())

    assert items_in_pool == [None, None, None, None, None]
