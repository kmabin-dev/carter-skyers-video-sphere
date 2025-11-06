import logging
import os
import pytest

from fan import Fan
import config


class _Flag:
    def __init__(self, value=False):
        self.value = value


class _BufferAlwaysFull:
    def __init__(self):
        self.vj_has_all_shards = _Flag(False)
        self.put_calls = 0
        self.failed = []
    def put_shard(self, name, path, timeout=None):
        self.put_calls += 1
        return False
    def register_failed_temp(self, path):
        self.failed.append(path)


class _BufferDjDone:
    def __init__(self):
        self.vj_has_all_shards = _Flag(True)
        self.put_calls = 0
        self.failed = []
    def put_shard(self, name, path, timeout=None):
        self.put_calls += 1
        return True
    def register_failed_temp(self, path):
        self.failed.append(path)


def test_enqueue_backoff_and_register_failed_temp(tmp_path, monkeypatch, caplog):
    # use test temp dir
    monkeypatch.setattr(config, 'TEMP_DIR', tmp_path)
    # speed up retries
    monkeypatch.setattr('time.sleep', lambda *_: None)
    caplog.set_level(logging.DEBUG, logger='config')

    buf = _BufferAlwaysFull()
    f = Fan(1, verbose=False)
    f.send_shard(buf)

    # expect multiple retries and an error at end
    debug_backpressure = [r for r in caplog.records if r.levelno == logging.DEBUG and 'backpressure' in r.getMessage()]
    assert len(debug_backpressure) >= 3
    error_logs = [r for r in caplog.records if r.levelno == logging.ERROR and 'failed to enqueue shard' in r.getMessage()]
    assert len(error_logs) == 1
    # fan should register the failed temp for cleanup
    assert len(buf.failed) == 1
    # path should no longer exist if cleanup fallback ran; but we accept registration here


def test_early_exit_when_dj_done_removes_temp(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(config, 'TEMP_DIR', tmp_path)
    caplog.set_level(logging.DEBUG, logger='config')

    buf = _BufferDjDone()
    f = Fan(2, verbose=True)
    # Run
    f.send_shard(buf)

    # Should not attempt to put into buffer
    assert buf.put_calls == 0
    # No failed registrations
    assert buf.failed == []
    # Temp dir should be empty (the created temp file removed)
    leftovers = list(tmp_path.iterdir())
    # There might be zero or more files if parallel tests run; ensure no stray .tmp created by this fan
    assert not any(p.name.startswith('tmp') for p in leftovers)
    # Check info message path
    has_info = any('detected DJ has capacity/full' in r.getMessage() and r.levelno == logging.INFO for r in caplog.records)
    assert has_info
