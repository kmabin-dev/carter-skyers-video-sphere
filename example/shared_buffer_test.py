
import multiprocessing

import config
import shared_buffer


def test_init():
    manager = multiprocessing.Manager()
    sh = shared_buffer.SharedBuffer(manager)
    assert len(sh.buffer()) == config.SHARED_BUFFER_SIZE


def test_str():
    manager = multiprocessing.Manager()
    sh = shared_buffer.SharedBuffer(manager)
    s = str(sh)
    assert s
