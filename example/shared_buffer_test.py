
import multiprocessing

import config
import shared_buffer


def test_init():
    manager = multiprocessing.Manager()
    sh = shared_buffer.SharedBuffer(manager)
    # Queue starts empty
    assert sh.qsize() == 0
    # Fill up to capacity
    for i in range(config.SHARED_BUFFER_SIZE):
        assert sh.put_shard(f'fan{i}', f'/tmp/dummy_{i}.mp4', timeout=0.1)
    assert sh.qsize() == config.SHARED_BUFFER_SIZE
    # Next put should fail quickly due to capacity
    assert not sh.put_shard('fanX', '/tmp/overflow.mp4', timeout=0.1)


def test_str():
    manager = multiprocessing.Manager()
    sh = shared_buffer.SharedBuffer(manager)
    s = str(sh)
    assert s
