from random import uniform

from typing_extensions import Literal

from remoulade.errors import UnknownStrategy

BackoffStrategy = Literal["constant", "linear", "spread_linear", "exponential", "spread_exponential"]


def compute_backoff(
    attempts: int,
    *,
    backoff_strategy: BackoffStrategy = "exponential",
    min_backoff: int = 5,
    max_backoff: int = 2000,
    max_retries: int = 32,
    jitter: bool = True,
):
    """Compute the backoff using the selected strategy.

    Parameters:
      attempts(int): The number of attempts there have been so far.
      backoff_strategy(str): The selected strategy.
      min_backoff(int): The minimum backoff duration in milliseconds.
      jitter(bool): If true adds a small random value to the backoff to avoid mass simultaneous retries.
      max_backoff(int): The max number of milliseconds to backoff by.
      max_retries(int): The maximum number of retries.
    """
    if backoff_strategy == "spread_exponential":
        backoff = compute_backoff_spread_exponential(
            attempts, min_backoff=min_backoff, max_backoff=max_backoff, max_retries=max_retries
        )
    elif backoff_strategy == "constant":
        backoff = min_backoff
    elif backoff_strategy == "linear":
        backoff = min((attempts + 1) * min_backoff, max_backoff)
    elif backoff_strategy == "spread_linear":
        backoff = compute_backoff_spread_linear(
            attempts, min_backoff=min_backoff, max_backoff=max_backoff, max_retries=max_retries
        )
    elif backoff_strategy == "exponential":
        backoff = compute_backoff_exponential(
            attempts, min_backoff=min_backoff, max_backoff=max_backoff, max_retries=max_retries
        )
    else:
        raise UnknownStrategy(f"Unknown retry strategy: {backoff_strategy}")
    if jitter:
        jitter_factor = 1 / 4
        backoff = int(backoff * (1 - jitter_factor) + uniform(0, backoff) * jitter_factor)

    return attempts + 1, backoff


def compute_backoff_spread_linear(attempts: int, *, min_backoff: int, max_backoff: int, max_retries: int):
    """Compute an linear backoff value linearly spread between min_backoff and max_backoff.

    Parameters:
      attempts(int): The number of attempts there have been so far.
      min_backoff(int): The minimum backoff duration in milliseconds.
      max_backoff(int): The max number of milliseconds to backoff by.
      max_retries(int): The maximum number of retries.

    Returns:
      int: The backoff in milliseconds.
    """
    if max_retries == 1:
        return min_backoff
    return min_backoff + (max_backoff - min_backoff) * min(attempts / (max_retries - 1), 1)


def compute_backoff_exponential(attempts: int, *, min_backoff: int, max_backoff: int, max_retries: int):
    """Compute an exponential backoff value based on some number of attempts.

    Parameters:
      attempts(int): The number of attempts there have been so far.
      min_backoff(int): The minimum backoff duration in milliseconds.
      max_backoff(int): The max number of milliseconds to backoff by.
      max_retries(int): The maximum number of retries.

    Returns:
      int: The backoff in milliseconds.
    """
    exponent = min(attempts, max_retries - 1)
    backoff = min(min_backoff * 2**exponent, max_backoff)
    return backoff


def compute_backoff_spread_exponential(attempts: int, *, min_backoff: int, max_backoff: int, max_retries: int):
    """Compute an exponential backoff value exponentially spread between min_backoff and max_backoff.

    Parameters:
      attempts(int): The number of attempts there have been so far.
      min_backoff(int): The minimum backoff duration in milliseconds.
      max_backoff(int): The max number of milliseconds to backoff by.
      max_retries(int): The maximum number of retries.

    Returns:
      int: The backoff in milliseconds.
    """

    if max_retries == 1:
        return min_backoff
    exponent = min(attempts / (max_retries - 1), 1)
    backoff = min_backoff * ((max_backoff / min_backoff) ** exponent)
    return backoff
