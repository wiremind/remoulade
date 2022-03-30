from typing import Any, Dict, Set

from ..backend import ForgottenResult, Missing, ResultBackend


class LocalBackend(ResultBackend):
    """An in-memory result backend. For use with LocalBroker only.

    We need to be careful here: if an actor store its results and never forget it, we may store all its results
    and never delete it. Resulting in a memory leak.
    """

    results: Dict[str, Any] = {}
    group_completions: Dict[str, Set[str]] = {}
    forgotten_results: Set[str] = set()

    def _get(self, message_key: str, forget: bool = False):
        if message_key in self.forgotten_results:
            return ForgottenResult.asdict()

        try:
            if forget:
                data = self.results.pop(message_key)
                self.forgotten_results.add(message_key)
                return data
            else:
                return self.results[message_key]
        except KeyError:
            return Missing

    def _store(self, message_keys, results, _):
        for (message_key, result) in zip(message_keys, results):
            self.results[message_key] = result

    def _delete(self, key: str):
        try:
            del self.results[key]
        except KeyError:
            pass

    def increment_group_completion(self, group_id: str, message_id: str, ttl: int) -> int:
        group_completion_key = self.build_group_completion_key(group_id)
        completed = self.group_completions.get(group_completion_key, set()) | {message_id}
        self.group_completions[group_completion_key] = completed
        return len(completed)
