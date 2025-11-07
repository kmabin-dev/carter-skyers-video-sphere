import multiprocessing

import fan
import shared_buffer


def test_init():
    f = fan.Fan(1)
    assert f.id() == 1
    assert f.buffer() is not None


def test_send_shard():
    f = fan.Fan(1)
    manager = multiprocessing.Manager()
    sh_buf = shared_buffer.SharedBuffer(manager)
    f.send_shard(sh_buf)


def test_str():
    f = fan.Fan(1)
    s = str(f)
    assert s
