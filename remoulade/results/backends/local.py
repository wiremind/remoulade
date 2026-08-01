import contextlib
from collections.abc import Iterable
from typing import Any, ClassVar

from ..backend import ForgottenResult, Missing, ResultBackend


class LocalBackend(ResultBackend):
    """An in-memory result backend. For use with LocalBroker only.

    We need to be careful here: if an actor store its results and never forget it, we may store all its results
    and never delete it. Resulting in a memory leak.
    """

    results: ClassVar[dict[str, Any]] = {}
    group_completions: ClassVar[dict[str, set[str]]] = {}
    forgotten_results: ClassVar[set[str]] = set()

    def _get(self, message_key: str, forget: bool = False):
        if message_key in self.forgotten_results:
            return ForgottenResult.asdict()

        try:
            if forget:
                data = self.results.pop(message_key)
                self.forgotten_results.add(message_key)
                return data
            return self.results[message_key]
        except KeyError:
            return Missing

    def _store(self, message_keys: Iterable[str], result: Any, ttl: int) -> None:
        for message_key, res in zip(message_keys, result, strict=False):
            self.results[message_key] = res

    def _delete(self, key: str):
        with contextlib.suppress(KeyError):
            del self.results[key]

    def increment_group_completion(self, group_id: str, message_id: str, ttl: int) -> int:
        group_completion_key = self.build_group_completion_key(group_id)
        completed = self.group_completions.get(group_completion_key, set()) | {message_id}
        self.group_completions[group_completion_key] = completed
        return len(completed)
