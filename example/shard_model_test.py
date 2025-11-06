import os
import tempfile

import pytest

import video
from shard import Shard


def test_shard_success_has_correct_hash_and_accessors():
    # Create a temporary file and compute its hash
    with tempfile.NamedTemporaryFile(delete=False) as tf:
        tf.write(b"hello world\n")
        tmp_path = tf.name

    try:
        expected_hash = video.file_hash(tmp_path)
        s = Shard(id=123, start=0.0, end=1.0, file_path=tmp_path, hash=expected_hash)

        assert s.id() == 123
        assert s.start() == 0.0
        assert s.end() == 1.0
        assert s.file_path() == tmp_path
        assert s.hash() == expected_hash
        # __str__ returns JSON-like content containing the id and file path
        text = str(s)
        assert '"id": 123' in text
        assert tmp_path in text
    finally:
        # Cleanup temp file
        try:
            os.remove(tmp_path)
        except OSError:
            pass


def test_shard_raises_on_hash_mismatch():
    with tempfile.NamedTemporaryFile(delete=False) as tf:
        tf.write(b"some content\n")
        tmp_path = tf.name

    try:
        wrong_hash = "deadbeef"
        with pytest.raises(Exception):
            Shard(id=1, start=0.0, end=1.0, file_path=tmp_path, hash=wrong_hash)
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
