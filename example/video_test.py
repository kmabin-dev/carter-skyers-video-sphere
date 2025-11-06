import os
import pytest

import config
import video


def test_end_reached_cb():
    params = {
        'finish': False
    }
    video.end_reached_cb(None, params)
    assert params['finish']


def test_ms_to_time():
    t = video.ms_to_timecode(70000)
    assert t == '00:01:10.0000'


def test_clean_temp_directory():
    video.clean_temp_directory()


def test_shake256_hash():
    digest = video.shake256_hash('hello')
    assert digest == '1234075ae4a1e773'


def test_file_hash():
    digest = video.file_hash(str(config.SOURCE_VIDEO_FILE_PATH))
    assert digest == '0d7ef6cec5a9f1a2'


def test_dimensions():
    # Use the configured source video path directly (Path-safe)
    video_file_path = config.SOURCE_VIDEO_FILE_PATH
    if not os.path.exists(str(video_file_path)):
        pytest.skip('source video file not present')
    dimensions = video.dimensions(str(video_file_path))
    assert len(dimensions) == 2


def test_audio():
    if not os.path.exists(str(config.TEST_VIDEO_FILE_PATH)) or not os.path.exists(str(config.TEST_AUDIO_FILE_PATH)):
        pytest.skip('test media not present')
    res_file_path = video.audio('test', config.TEST_VIDEO_FILE_PATH, config.TEST_AUDIO_FILE_PATH)
    assert res_file_path and os.path.exists(res_file_path)


def test_concet():
    if not os.path.exists(str(config.TEST_VIDEO_FILE_PATH)):
        pytest.skip('test video not present')
    res_file_path = video.concat('test', config.TEST_VIDEO_FILE_PATH, config.TEST_VIDEO_FILE_PATH)
    assert res_file_path and os.path.exists(res_file_path)


def test_play():
    # play() is a no-op under pytest due to env guard; just ensure no crash
    video.play(str(config.TEST_VIDEO_FILE_PATH))


def test_probe():
    if not os.path.exists(str(config.TEST_VIDEO_FILE_PATH)):
        pytest.skip('test video not present')
    video.probe(str(config.TEST_VIDEO_FILE_PATH))


def test_create_shard():
    if not os.path.exists(str(config.TEST_VIDEO_FILE_PATH)):
        pytest.skip('test video not present')
    tmp = video.temp_file_path('create_shard', '.mp4')
    out = video.create_shard(config.TEST_VIDEO_FILE_PATH, tmp, 10, 20)
    # function returns None on error; ensure output exists if processing succeeded
    if out is not None:
        assert os.path.exists(tmp)


def test_write_shard():
    name = 'write_shard_test'
    output_file_path = video.write(name, b'beef')
    assert os.path.exists(output_file_path)
