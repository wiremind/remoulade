from ..backend import Missing, ResultBackend


class LocalBackend(ResultBackend):
    """An in-memory result backend. For use with LocalBroker only.

    We need to be careful here: if an actor store its results and never retrieves it, we may store all its results
    and never delete it. Resulting in a memory leak.
    """

    results = {}

    def _get(self, message_key, forget: bool = False):
        try:
            if forget:
                return self.results.pop(message_key)
            else:
                return self.results[message_key]
        except KeyError:
            return Missing

    def _store(self, message_key, result, _):
        self.results[message_key] = result
