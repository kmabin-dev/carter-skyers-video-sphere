import os
import logging
import pytest
from fan import Fan
import config


class _Flag:
    def __init__(self, value=False):
        self.value = value


class _FakeBuffer:
    def __init__(self):
        self.vj_has_all_shards = _Flag(False)
        self.items = []

    def put_shard(self, name, path, timeout=None):
        self.items.append((name, path))
        return True

    def register_failed_temp(self, path):
        pass


def test_only_first_8_fans_log_info(tmp_path, caplog, monkeypatch):
    # Redirect temp dir to test dir
    monkeypatch.setattr(config, "TEMP_DIR", tmp_path)

    buf = _FakeBuffer()
    # Ensure the 'config' logger (used by modules) captures DEBUG
    caplog.set_level(logging.DEBUG, logger="config")

    # Create 16 fans; only first 8 verbose
    fans = [Fan(i, verbose=(i < 8)) for i in range(16)]
    for f in fans:
        f.send_shard(buf)

    # Count INFO vs DEBUG logs for the send event
    info_msgs = [
        r
        for r in caplog.records
        if r.levelno == logging.INFO
        and "sent shard -> shared buffer" in r.getMessage()
    ]
    debug_msgs = [
        r
        for r in caplog.records
        if r.levelno == logging.DEBUG
        and "sent shard -> shared buffer" in r.getMessage()
    ]

    assert len(info_msgs) == 8
    assert len(debug_msgs) == 8
