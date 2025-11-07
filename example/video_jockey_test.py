import multiprocessing
from unittest.mock import patch, patch as mock_patch
from unittest import mock
import tempfile
import os

import config
import shared_buffer as sb
import video_jockey
from config import logger


def test_init():
    vj = video_jockey.VideoJockey()
    assert vj


def test_name():
    vj = video_jockey.VideoJockey()
    assert vj.name() == "Marshmello"


def test_shards():
    vj = video_jockey.VideoJockey()
    vj.shards()[0] = b"beef"
    assert len(vj.shards()) == 1


def test_str_():
    vj = video_jockey.VideoJockey()
    s = str(vj)
    assert len(s) > 0


# patch (mock) constants for unit test
@patch("config.SHARED_BUFFER_SIZE", 2)
@patch("config.NUM_SHARDS", 2)
def test_read_all_shards():
    vj = video_jockey.VideoJockey()
    manager = multiprocessing.Manager()
    shared_buffer = sb.SharedBuffer(manager)
    # create temp files to simulate shard paths
    tmp1 = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
    tmp2 = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
    tmp1.write(b"fake-mp4-1")
    tmp1.flush()
    tmp1.close()
    tmp2.write(b"fake-mp4-2")
    tmp2.flush()
    tmp2.close()
    assert shared_buffer.put_shard("fan0", tmp1.name, timeout=0.1)
    assert shared_buffer.put_shard("fan1", tmp2.name, timeout=0.1)
    assert vj._VideoJockey__read_all_shards(shared_buffer, total_shards=2)


def read_test_shard():
    file_path = config.TEST_VIDEO_FILE_PATH
    try:
        file = open(file_path, "rb")
    except Exception as e:
        logger.error(
            f"Unable to open {file_path} exception={type(e).__name__}"
        )
        quit(-1)

    # read the data
    byte_data = file.read()

    # close file
    file.close()

    return byte_data


# patch (mock) constants for unit test


@patch("config.SHARED_BUFFER_SIZE", 2)
@patch("config.NUM_SHARDS", 2)
def test_start():

    # setup the vj
    vj = video_jockey.VideoJockey()
    manager = multiprocessing.Manager()
    shared_buffer = sb.SharedBuffer(manager)
    # create temp shard files and enqueue paths
    tmp1 = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
    tmp2 = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
    tmp1.write(b"fake-mp4-1")
    tmp1.flush()
    tmp1.close()
    tmp2.write(b"fake-mp4-2")
    tmp2.flush()
    tmp2.close()
    assert shared_buffer.put_shard("fan0", tmp1.name, timeout=0.1)
    assert shared_buffer.put_shard("fan1", tmp2.name, timeout=0.1)

    # Avoid running real ffmpeg; patch write method to a no-op that returns a dummy path
    with mock.patch.object(
        video_jockey.VideoJockey,
        f"_VideoJockey__write_video",
        return_value="/tmp/final.mp4",
    ):
        vj.start(shared_buffer, total_shards=2)

    # cleanup temps
    for p in (tmp1.name, tmp2.name):
        try:
            os.remove(p)
        except OSError:
            pass
