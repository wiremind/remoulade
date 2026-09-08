from limits.storage import Storage
from limits.strategies import (
    FixedWindowRateLimiter,
    MovingWindowRateLimiter,
    RateLimiter,
    SlidingWindowCounterRateLimiter,
)


def build_limiter(storage: Storage, *, strategy: str) -> RateLimiter:
    """Return a limits limiter instance for the configured strategy."""
    if strategy == "fixed_window":
        return FixedWindowRateLimiter(storage)
    if strategy == "moving_window":
        return MovingWindowRateLimiter(storage)
    if strategy == "sliding_window":
        return SlidingWindowCounterRateLimiter(storage)
    available_strategies = {"fixed_window", "moving_window", "sliding_window"}
    available = ", ".join(sorted(available_strategies))
    raise ValueError(f"Unknown rate limit strategy {strategy!r}. Available: {available}")
