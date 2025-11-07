import os
import subprocess
import pytest
from unittest import mock
import video
import config


def _dummy_video(tmp_path, name="d.mp4"):
    # create a tiny dummy mp4 file (empty file acceptable for our mocked tests)
    p = tmp_path / name
    with open(p, "wb") as fh:
        fh.write(b"\x00\x00")
    return p


def test_probe_success():
    # Skip if ffprobe not available
    if (
        subprocess.run(
            ["which", "ffprobe"], capture_output=True, text=True, check=False
        ).returncode
        != 0
    ):
        pytest.skip("ffprobe not available")
    # Use real source if exists else skip
    if not os.path.exists(str(config.SOURCE_VIDEO_FILE_PATH)):
        pytest.skip("source video missing")
    info = video.probe(str(config.SOURCE_VIDEO_FILE_PATH))
    assert "codec_type" in info and info["codec_type"] == "video"


def test_probe_failure():
    with mock.patch("subprocess.run") as run:
        run.returncode = 1
        run.return_value.returncode = 1
        run.return_value.stderr = "boom"
        with pytest.raises(RuntimeError):
            video.probe("/non/existent/file.mp4")


@mock.patch("subprocess.run")
def test_audio_mux_failure(run, tmp_path):
    # Simulate ffmpeg failure
    run.return_value.returncode = 1
    run.return_value.stderr = "error"
    out = video.audio("x", tmp_path / "v.mp4", tmp_path / "a.mp3")
    assert out is None


@mock.patch("subprocess.run")
def test_audio_mux_success(run, tmp_path):
    run.return_value.returncode = 0
    run.return_value.stderr = ""
    v = _dummy_video(tmp_path, "v.mp4")
    a = _dummy_video(tmp_path, "a.mp3")
    out = video.audio("x", v, a)
    assert out is not None


@mock.patch("subprocess.run")
def test_concat_creates_list_and_calls_ffmpeg(run, tmp_path):
    run.return_value.returncode = 0
    v1 = _dummy_video(tmp_path, "v1.mp4")
    v2 = _dummy_video(tmp_path, "v2.mp4")
    out = video.concat("c", v1, v2)
    assert run.called
    assert out is not None


@mock.patch("subprocess.run")
def test_concat_failure(run, tmp_path):
    run.return_value.returncode = 1
    run.return_value.stderr = "fail"
    v1 = _dummy_video(tmp_path, "v1.mp4")
    v2 = _dummy_video(tmp_path, "v2.mp4")
    out = video.concat("c", v1, v2)
    assert out is None


@mock.patch("subprocess.run")
def test_create_shard_success(run, tmp_path):
    run.return_value.returncode = 0
    src = _dummy_video(tmp_path, "src.mp4")
    out = tmp_path / "out.mp4"
    video.create_shard(src, str(out), 1, 2)
    assert run.called
    # function returns None only on error; success keeps None but file should not exist because we mocked
    # we just assert run invocation


@mock.patch("subprocess.run")
def test_create_shard_failure(run, tmp_path):
    run.return_value.returncode = 1
    src = _dummy_video(tmp_path, "src.mp4")
    out = tmp_path / "out.mp4"
    res = video.create_shard(src, str(out), 1, 2)
    assert res is None


def test_temp_file_path_generates_unique():
    p1 = video.temp_file_path("abc", ".mp4")
    p2 = video.temp_file_path("abc", ".mp4")
    assert p1 != p2
