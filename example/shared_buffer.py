"""
SharedBuffer: a small bounded buffer for shard paths implemented
with a manager.Queue. This simplifies concurrency (no manual per-
slot locks) and provides natural backpressure when the buffer is
full.

API:
                - put_shard(sender_name, file_path, timeout): try to enqueue,
                        return True/False
                - get_shard(timeout): try to dequeue, return (sender_name,
                        file_path) or None
                - vj_has_all_shards: manager.Value('b') flag set by the DJ when
                        collection done
"""

import config
import queue


class SharedBuffer(object):
    def __init__(self, manager):
        # flag set by VideoJockey when it has collected all shards
        self.vj_has_all_shards = manager.Value("b", False)

        # bounded queue for shard entries (sender_name, file_path)
        # Use manager.Queue so it is safe across processes
        self._queue = manager.Queue(maxsize=config.SHARED_BUFFER_SIZE)
        # list of temp file paths that failed to be enqueued and need
        # cleanup. Producers register failed temp files here instead
        # of deleting them immediately so a separate cleanup worker
        # can remove them safely.
        self.failed_temp_paths = manager.list()

    def put_shard(self, sender_name, file_path, timeout=5.0):
        """Try to put a shard into the queue. Returns True on success.

        If the queue is full, this will block up to `timeout` seconds
        and then return False if not possible.
        """
        try:
            self._queue.put((sender_name, file_path), timeout=timeout)
            return True
        except queue.Full:
            return False
        except (EOFError, BrokenPipeError, OSError):
            return False

    def get_shard(self, timeout=0.1):
        """Try to get a shard from the queue.

        Returns (sender_name, file_path) or None on timeout.
        """
        try:
            item = self._queue.get(timeout=timeout)
            return item
        except queue.Empty:
            return None
        except (EOFError, BrokenPipeError, OSError):
            return None

    def qsize(self):
        try:
            return self._queue.qsize()
        except (NotImplementedError, OSError, AttributeError):
            return 0

    def register_failed_temp(self, temp_path):
        """Producers call this to register a temp file that couldn't
        be enqueued. The cleanup worker will later attempt to remove
        these.
        """
        try:
            # Avoid duplicates
            if temp_path not in self.failed_temp_paths:
                self.failed_temp_paths.append(temp_path)
        except (
            AttributeError,
            ValueError,
            EOFError,
            BrokenPipeError,
            OSError,
        ):
            pass

    def get_and_clear_failed_temps(self):
        """Return a snapshot list of failed temp paths and clear the
        list in a single operation.
        """
        try:
            snapshot = list(self.failed_temp_paths)
            # clear the manager list
            del self.failed_temp_paths[:]
            return snapshot
        except (
            AttributeError,
            ValueError,
            EOFError,
            BrokenPipeError,
            OSError,
        ):
            return []

    # Compatibility helper: older tests may call buffer(), keep it but
    # mark as legacy
    def buffer(self):
        return self._queue
