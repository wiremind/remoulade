import subprocess
import sys

# A None entry in sys.modules makes every "import limits..." raise ImportError.
BLOCK_LIMITS = "import sys; sys.modules['limits'] = None\n"

BUILD_RATE_LIMIT_MIDDLEWARE = """
from remoulade.rate_limits import RateLimitBackend, RateLimitEnqueue

class Backend(RateLimitBackend):
    pass

try:
    RateLimitEnqueue(backend=Backend())
except ImportError as exc:
    assert "remoulade[limits]" in str(exc), exc
else:
    raise AssertionError("RateLimitEnqueue was built without limits")
"""


def run_without_limits(code: str) -> None:
    subprocess.run([sys.executable, "-c", BLOCK_LIMITS + code], check=True, timeout=60)


def test_remoulade_imports_without_the_limits_extra():
    run_without_limits("import remoulade, remoulade.rate_limits, remoulade.brokers.stub")


def test_rate_limit_middleware_needs_the_limits_extra():
    run_without_limits(BUILD_RATE_LIMIT_MIDDLEWARE)
