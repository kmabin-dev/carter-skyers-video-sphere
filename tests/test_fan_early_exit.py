import logging
from pathlib import Path
import sys

import pytest

# Ensure project root is on sys.path so tests can import local modules
root = Path(__file__).resolve().parents[1]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

import config  # noqa: E402
from fan import Fan  # noqa: E402


class _Flag:
    def __init__(self, value=False):
        self.value = value


class _ImmediateDoneBuffer:
    def __init__(self):
        self.vj_has_all_shards = _Flag(True)
        self.put_calls = 0

    def put_shard(self, *_args, **_kwargs):
        self.put_calls += 1
        return False


def test_fan_detects_vj_done_and_removes_temp(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
):
    # Redirect temp dir to the tmp path
    monkeypatch.setattr(config, "TEMP_DIR", tmp_path)

    buf = _ImmediateDoneBuffer()

    caplog.set_level(logging.DEBUG, logger="config")

    f = Fan(1, verbose=True)
    f.send_shard(buf)

    # DJ completion path should log and not call put_shard
    assert buf.put_calls == 0

    # The temp dir should remain empty (temp file was removed)
    assert list(tmp_path.iterdir()) == []
