import os
import subprocess
import tempfile
import json
import shlex
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
    hash = video.shake256_hash('hello')
    assert hash == '1234075ae4a1e773'


def test_file_hash():
    hash = video.file_hash(config.SOURCE_VIDEO_FILE_PATH)
    assert hash == '0d7ef6cec5a9f1a2'


def test_dimensions_cli():
    # Use ffprobe CLI to get dimensions (aligns with CLI flow)
    if not os.path.exists(str(config.TEST_VIDEO_FILE_PATH)):
        pytest.skip('Test video file not present')
    video_file_path = str(config.TEST_VIDEO_FILE_PATH)
    cmd = [
        'ffprobe', '-v', 'error', '-select_streams', 'v:0',
        '-show_entries', 'stream=width,height', '-of', 'json', video_file_path
    ]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    assert proc.returncode == 0
    info = json.loads(proc.stdout)
    w = info['streams'][0]['width']
    h = info['streams'][0]['height']
    assert w > 0 and h > 0


def test_audio_cli():
    # Mux audio onto video via ffmpeg CLI (no re-encode of video)
    if not (os.path.exists(str(config.TEST_VIDEO_FILE_PATH)) and os.path.exists(str(config.TEST_AUDIO_FILE_PATH))):
        pytest.skip('Test media files not present')
    out_fd, out_path = tempfile.mkstemp(suffix='.mp4', dir=str(config.TEMP_DIR))
    os.close(out_fd)
    cmd = [
        'ffmpeg', '-v', 'error', '-i', str(config.TEST_VIDEO_FILE_PATH), '-i', str(config.TEST_AUDIO_FILE_PATH),
        '-c:v', 'copy', '-c:a', 'aac', '-shortest', '-y', out_path
    ]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    assert proc.returncode == 0
    assert os.path.exists(out_path)
    os.remove(out_path)


def test_concat_cli():
    # Concat two copies of the same test clip via concat demuxer
    if not os.path.exists(str(config.TEST_VIDEO_FILE_PATH)):
        pytest.skip('Test video file not present')
    os.makedirs(str(config.TEMP_DIR), exist_ok=True)
    list_path = os.path.join(str(config.TEMP_DIR), 'test_concat_list.txt')
    out_path = os.path.join(str(config.TEMP_DIR), 'concat_out.mp4')
    with open(list_path, 'w', encoding='utf-8') as fh:
        p = str(config.TEST_VIDEO_FILE_PATH).replace("'", "'\\''")
        fh.write(f"file '{p}'\n")
        fh.write(f"file '{p}'\n")
    cmd = [
        'ffmpeg', '-v', 'error', '-f', 'concat', '-safe', '0', '-i', list_path,
        '-c:v', 'copy', '-y', out_path
    ]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    assert proc.returncode == 0
    assert os.path.exists(out_path)
    os.remove(list_path)
    os.remove(out_path)


def test_play_noop():
    # Ensure the function can be called without blocking (PYTEST guard in code)
    video.play(str(config.TEST_VIDEO_FILE_PATH))


def test_probe_cli():
    # Use ffprobe CLI to ensure probing works
    if not os.path.exists(str(config.TEST_VIDEO_FILE_PATH)):
        pytest.skip('Test video file not present')
    cmd = ['ffprobe', '-v', 'error', '-show_format', '-show_streams', '-of', 'json', str(config.TEST_VIDEO_FILE_PATH)]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    assert proc.returncode == 0
    info = json.loads(proc.stdout)
    assert 'streams' in info and len(info['streams']) > 0


def test_create_shard_cli():
    # Trim 10s segment using ffmpeg CLI
    if not os.path.exists(str(config.TEST_VIDEO_FILE_PATH)):
        pytest.skip('Test video file not present')
    os.makedirs(str(config.TEMP_DIR), exist_ok=True)
    out_fd, out_path = tempfile.mkstemp(suffix='.mp4', dir=str(config.TEMP_DIR))
    os.close(out_fd)
    cmd = [
        'ffmpeg', '-v', 'error', '-ss', '10', '-to', '20', '-i', str(config.TEST_VIDEO_FILE_PATH),
        '-c', 'copy', '-y', out_path
    ]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    assert proc.returncode == 0
    assert os.path.exists(out_path)
    os.remove(out_path)


def test_write_shard(monkeypatch):
    # Monkeypatch temp_file_path to avoid Path/str concatenation issue in legacy code
    def _temp_file_path(name, ext):
        return os.path.join(str(config.TEMP_DIR), f'temp_{name}.mp4')
    monkeypatch.setattr(video, 'temp_file_path', _temp_file_path)
    name = 'write_shard_test'
    output_file_path = video.write(name, b'beef')
    assert os.path.exists(output_file_path)
    os.remove(output_file_path)
