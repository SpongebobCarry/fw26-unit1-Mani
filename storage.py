import threading
from collections import deque


class KeyValueStore:
    """Store values in a dictionary."""

    def __init__(self):
        self._data = {}
        self._lock = threading.Lock()

    def put(self, key, value):
        """Add or update a value."""

        with self._lock:
            self._data[key] = value

    def get(self, key):
        """Get a value using its key."""

        with self._lock:
            return self._data.get(key)


class TaskQueue:
    """Keep tasks in the order they are added."""

    def __init__(self):
        self._queue = deque()
        self._condition = threading.Condition()

    def enqueue(self, task):
        """Add a task to the queue."""

        with self._condition:
            self._queue.append(task)

            # Wake up a waiting thread
            self._condition.notify()

    def dequeue(self):
        """Get the next task from the queue."""

        with self._condition:
            while not self._queue:
                self._condition.wait()

            return self._queue.popleft()
