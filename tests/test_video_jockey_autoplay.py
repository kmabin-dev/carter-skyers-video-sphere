import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

# Ensure example/ is on sys.path
example_dir = Path(__file__).resolve().parents[1] / "example"
if str(example_dir) not in sys.path:
    sys.path.insert(0, str(example_dir))

import config  # noqa: E402
from video_jockey import VideoJockey  # noqa: E402


class _DummyBuffer:
    pass


def test_autoplay_invoked_when_enabled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    vj = VideoJockey()

    # Enable autoplay
    monkeypatch.setattr(config, "AUTO_PLAY_FINAL_VIDEO", True, raising=True)

    # Pretend reading shards succeeded
    monkeypatch.setattr(
        vj, "_VideoJockey__read_all_shards", lambda *_: True, raising=True
    )

    # Stub write_video to return a real file path
    out_path = tmp_path / "final.mp4"
    out_path.write_bytes(b"00")
    monkeypatch.setattr(
        vj, "_VideoJockey__write_video", lambda: str(out_path), raising=True
    )

    calls = []

    class _PopenSpy:
        def __init__(self, args):
            calls.append(args)

    # Intercept subprocess.Popen used for 'open <file>'
    monkeypatch.setattr(
        "video_jockey.subprocess.Popen", _PopenSpy, raising=True
    )

    vj.start(_DummyBuffer(), total_shards=1)

    assert calls, "subprocess.Popen should be called for autoplay"
    assert calls[0] == ["open", str(out_path)]
