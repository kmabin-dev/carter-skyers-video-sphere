
import multiprocessing
from unittest.mock import patch

import config
import shared_buffer as sb
import video_jockey
from config import logger


def test_init():
    vj = video_jockey.VideoJockey()
    assert vj


def test_name():
    vj = video_jockey.VideoJockey()
    assert vj.name() == 'Marshmello'


def test_shards():
    vj = video_jockey.VideoJockey()
    vj.shards()[0] = b'beef'
    assert len(vj.shards()) == 1


def test_str_():
    vj = video_jockey.VideoJockey()
    s = str(vj)
    assert len(s) > 0


# patch (mock) constants for unit test
@patch('config.SHARED_BUFFER_SIZE', 2)
@patch('config.NUM_SHARDS', 2)
def test_read_all_shards():
    vj = video_jockey.VideoJockey()
    manager = multiprocessing.Manager()
    shared_buffer = sb.SharedBuffer(manager)
    for i in range(config.SHARED_BUFFER_SIZE):
        lock = manager.Lock()
        lock.acquire()
        shared_buffer.buffer()[i] = (lock, f'fan{i}', b'beef')
    vj._VideoJockey__read_all_shards(shared_buffer)


def read_test_shard():
    file_path = config.TEST_VIDEO_FILE_PATH
    try:
        file = open(file_path, 'rb')
    except Exception as e:
        logger.error(
            f'Unable to open {file_path} exception={type(e).__name__}')
        quit(-1)

    # read the data
    byte_data = file.read()

    # close file
    file.close()

    return byte_data

# patch (mock) constants for unit test


@patch('config.SHARED_BUFFER_SIZE', 2)
@patch('config.NUM_SHARDS', 2)
def test_start():

    # read a test shard
    byte_data = read_test_shard()

    # setup the vj
    vj = video_jockey.VideoJockey()
    manager = multiprocessing.Manager()
    shared_buffer = sb.SharedBuffer(manager)
    for i in range(config.SHARED_BUFFER_SIZE):
        lock = manager.Lock()
        lock.acquire()
        shared_buffer.buffer()[i] = (lock, f'fan{i}', byte_data)
    vj.start(shared_buffer)
