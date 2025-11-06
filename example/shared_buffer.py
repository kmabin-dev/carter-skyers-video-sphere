"""
SharedBuffer: a small bounded buffer for shard paths implemented with a
manager.Queue. This simplifies concurrency (no manual per-slot locks) and
provides natural backpressure when the buffer is full.

API:
  - put_shard(sender_name, file_path, timeout): try to enqueue, return True/False
  - get_shard(timeout): try to dequeue, return (sender_name, file_path) or None
  - vj_has_all_shards: manager.Value('b') flag set by the DJ when collection done
"""

import config


class SharedBuffer(object):
    def __init__(self, manager):
        # flag set by VideoJockey when it has collected all shards
        self.vj_has_all_shards = manager.Value('b', False)

        # bounded queue for shard entries (sender_name, file_path)
        # Use manager.Queue so it is safe across processes
        self._queue = manager.Queue(maxsize=config.SHARED_BUFFER_SIZE)

    def put_shard(self, sender_name, file_path, timeout=5.0):
        """Try to put a shard into the queue. Returns True on success.

        If the queue is full, this will block up to `timeout` seconds and then
        return False if not possible.
        """
        try:
            self._queue.put((sender_name, file_path), timeout=timeout)
            return True
        except Exception:
            return False

    def get_shard(self, timeout=0.1):
        """Try to get a shard from the queue.

        Returns (sender_name, file_path) or None on timeout.
        """
        try:
            item = self._queue.get(timeout=timeout)
            return item
        except Exception:
            return None

    def qsize(self):
        try:
            return self._queue.qsize()
        except Exception:
            return 0

    # Compatibility helper: older tests may call buffer(), keep it but mark as legacy
    def buffer(self):
        return self._queue
