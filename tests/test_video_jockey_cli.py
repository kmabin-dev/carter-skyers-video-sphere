import os
from unittest import mock
import video_jockey as vj_mod
import config


def test_ffmpeg_cmd_contains_offset_and_fades_and_cleans_list(tmp_path, monkeypatch):
    # Isolate temp dir
    monkeypatch.setattr(config, 'TEMP_DIR', tmp_path)
    # Configure audio params
    monkeypatch.setattr(config, 'AUDIO_OFFSET_SECONDS', 12)
    monkeypatch.setattr(config, 'AUDIO_FADE_IN_SECONDS', 1.0)
    monkeypatch.setattr(config, 'AUDIO_FADE_OUT_SECONDS', 2.3)
    monkeypatch.setattr(config, 'AUDIO_BITRATE', '192k')
    monkeypatch.setattr(config, 'SOURCE_AUDIO_FILE_PATH', tmp_path / 'song.mp3')

    # Create dummy shards
    s1 = tmp_path / 's1.mp4'
    s2 = tmp_path / 's2.mp4'
    s1.write_bytes(b'00')
    s2.write_bytes(b'00')

    vj = vj_mod.VideoJockey()
    # inject shards directly
    vj._VideoJockey__shards = [str(s1), str(s2)]

    captured = {}

    class FakeProc:
        def __init__(self):
            self.stderr = ['ffmpeg: starting']
        def wait(self):
            return 0

    def fake_popen(cmd, stdout=None, stderr=None, text=None):
        captured['cmd'] = cmd
        return FakeProc()

    with mock.patch('subprocess.Popen', side_effect=fake_popen):
        out = vj._VideoJockey__write_video()

    # Verify output path
    assert out == os.path.join(str(tmp_path), 'final_collage.mp4')
    # Concat list should be cleaned up
    assert not os.path.exists(tmp_path / 'concat_list.txt')
    # Shards should be cleaned up
    assert not s1.exists() and not s2.exists()

    # Verify ffmpeg command contains proper args
    cmd = captured['cmd']
    assert '-ss' in cmd and str(12) in cmd
    assert '-af' in cmd
    af_value = cmd[cmd.index('-af') + 1]
    assert 'afade=t=in:d=1.0' in af_value and 'afade=t=in:d=2.3' in af_value
