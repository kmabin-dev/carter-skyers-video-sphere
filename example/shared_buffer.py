import os
import uuid
import config


class SharedBuffer(object):
    """
    Shared buffer using per-slot locks and occupancy metadata.

    Each slot is a tuple (lock, sender_name, file_path) stored in a Manager.list.
    Writers acquire a slot's lock before checking/setting contents. Readers do
    the same when consuming.
    """

    def __init__(self, manager):
        # flag set by VideoJockey when it has collected all shards
        self.vj_has_all_shards = manager.Value('b', False)

        # initialize buffer slots as list of tuples: (lock, sender_name, file_path)
        self.__buffer = manager.list()
        for i in range(config.SHARED_BUFFER_SIZE):
            lock = manager.Lock()
            elem = (lock, None, None)
            self.__buffer.append(elem)

    def lock(self, i):
        return self.__buffer[i][0]

    def _get_slot(self, i):
        return self.__buffer[i]

    def _set_slot(self, i, sender, file_path):
        lock = self.__buffer[i][0]
        self.__buffer[i] = (lock, sender, file_path)

    def try_acquire_free_slot(self):
        """Try to acquire a free slot's lock (non-blocking).

        If successful and the slot is empty, returns the slot index with the
        lock still held by the caller. If no free slot, returns None.
        """
        for i in range(config.SHARED_BUFFER_SIZE):
            lock = self.lock(i)
            acquired = lock.acquire(False)
            if not acquired:
                continue
            # we hold the lock; check if empty
            _, sender, file_path = self._get_slot(i)
            if sender is None and file_path is None:
                # keep lock held; caller must call write_slot to populate and release
                return i
            # not free, release and continue
            lock.release()
        return None

    def write_slot(self, i, sender_name, file_path):
        """Write into slot i. Caller must hold slot lock."""
        # we assume caller holds the lock
        self._set_slot(i, sender_name, file_path)
        # release lock after writing
        self.lock(i).release()

    def read_any_slot_nonblocking(self):
        """Try to read any occupied slot without blocking.

        Returns (slot_index, sender_name, file_path) if found, otherwise None.
        """
        for i in range(config.SHARED_BUFFER_SIZE):
            lock = self.lock(i)
            acquired = lock.acquire(False)
            if not acquired:
                continue
            _, sender, file_path = self._get_slot(i)
            if sender is not None and file_path is not None:
                # consume the slot
                self._set_slot(i, None, None)
                lock.release()
                return (i, sender, file_path)
            lock.release()
        return None

    def buffer(self):
        return self.__buffer
