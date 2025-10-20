import os

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
    hash = video.shake256_hash('hello')
    assert hash == '1234075ae4a1e773'


def test_file_hash():
    hash = video.file_hash(config.SOURCE_VIDEO_FILE_PATH)
    assert hash == '0d7ef6cec5a9f1a2'


def test_dimensions():
    video_file_path = config.PROJECT_DIR + \
        '/video/carter-skyers-easiest-goodbye-official-music-video.mp4'
    dimensions = video.dimensions(video_file_path)
    assert len(dimensions) == 2


def test_audio():
    res_file_path = video.audio(
        'test', config.TEST_VIDEO_FILE_PATH, config.TEST_AUDIO_FILE_PATH)
    assert os.path.exists(res_file_path)


def test_concet():
    res_file_path = video.concat(
        'test', config.TEST_VIDEO_FILE_PATH, config.TEST_VIDEO_FILE_PATH)
    assert os.path.exists(res_file_path)


def test_play():
    video.play(config.TEST_VIDEO_FILE_PATH)


def test_probe():
    video.probe(config.TEST_VIDEO_FILE_PATH)


def test_create_shard():
    tmp = video.temp_file_path('create_shard', '.mp4')
    video.create_shard(config.TEST_VIDEO_FILE_PATH, tmp, 10, 20)


def test_write_shard():
    name = 'write_shard_test'
    output_file_path = video.write(name, b'beef')
    assert os.path.exists(output_file_path)
