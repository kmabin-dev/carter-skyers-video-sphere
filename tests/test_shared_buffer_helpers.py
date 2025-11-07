import sys
from pathlib import Path
from multiprocessing import Manager

import pytest

# Add example/ to sys.path to import example modules
example_dir = Path(__file__).resolve().parents[1] / "example"
if str(example_dir) not in sys.path:
    sys.path.insert(0, str(example_dir))

from shared_buffer import SharedBuffer  # noqa: E402


@pytest.fixture
def mp_manager():
    mgr = Manager()
    yield mgr
    mgr.shutdown()


def test_failed_temp_register_and_clear(mp_manager, tmp_path):
    sb = SharedBuffer(mp_manager)

    p1 = tmp_path / "a.mp4"
    p2 = tmp_path / "b.mp4"
    p1.write_bytes(b"x")
    p2.write_bytes(b"y")

    sb.register_failed_temp(str(p1))
    sb.register_failed_temp(str(p2))
    sb.register_failed_temp(str(p1))  # duplicate

    snapshot = sb.get_and_clear_failed_temps()
    assert set(snapshot) == {str(p1), str(p2)}

    # After clearing, list should be empty
    assert sb.get_and_clear_failed_temps() == []
