"""
Unit tests for SharedBuffer with new cleanup and capacity features.
"""
import pytest
import os
import sys
import tempfile
from pathlib import Path
from multiprocessing import Manager

# Add parent directory to path so we can import example modules
sys.path.insert(0, str(Path(__file__).parent.parent / 'example'))

from shared_buffer import SharedBuffer
import config


@pytest.fixture
def manager():
    """Provide a multiprocessing Manager for tests."""
    mgr = Manager()
    yield mgr
    mgr.shutdown()


@pytest.fixture
def shared_buffer(manager):
    """Provide a SharedBuffer instance for tests."""
    return SharedBuffer(manager)


@pytest.fixture
def temp_file():
    """Create a temporary test file and clean it up after the test."""
    tmp = tempfile.NamedTemporaryFile(delete=False, dir=str(config.TEMP_DIR), suffix='.mp4')
    tmp.write(b'test shard content')
    tmp.close()
    yield tmp.name
    # Clean up after test
    try:
        if os.path.exists(tmp.name):
            os.remove(tmp.name)
    except Exception:
        pass


def test_register_failed_temp(shared_buffer, temp_file):
    """Test that failed temp files can be registered."""
    # Register a failed temp file
    shared_buffer.register_failed_temp(temp_file)
    
    # Retrieve and verify it's in the list
    failed_temps = shared_buffer.get_and_clear_failed_temps()
    assert temp_file in failed_temps, "Failed temp should be in the list"
    
    # Verify list is cleared after retrieval
    failed_temps_again = shared_buffer.get_and_clear_failed_temps()
    assert len(failed_temps_again) == 0, "Failed temps list should be empty after clear"


def test_register_failed_temp_no_duplicates(shared_buffer, temp_file):
    """Test that duplicate registrations are avoided."""
    shared_buffer.register_failed_temp(temp_file)
    shared_buffer.register_failed_temp(temp_file)  # Register same path again
    
    failed_temps = shared_buffer.get_and_clear_failed_temps()
    assert failed_temps.count(temp_file) == 1, "Should not have duplicates"


def test_vj_capacity_flag(shared_buffer):
    """Test that vj_has_all_shards flag works correctly."""
    # Initially should be False
    assert not shared_buffer.vj_has_all_shards.value, "Flag should start as False"
    
    # Set to True (simulating DJ completion)
    shared_buffer.vj_has_all_shards.value = True
    assert shared_buffer.vj_has_all_shards.value, "Flag should be True after setting"


def test_bounded_capacity(shared_buffer):
    """Test that buffer respects capacity limit (config.SHARED_BUFFER_SIZE)."""
    capacity = config.SHARED_BUFFER_SIZE  # Should be 4
    
    # Fill the buffer to capacity
    for i in range(capacity):
        result = shared_buffer.put_shard(f"fan_{i}", f"path_{i}", timeout=0.1)
        assert result, f"Should be able to put shard {i} (capacity={capacity})"
    
    # Next put should fail due to capacity limit
    result = shared_buffer.put_shard("fan_overflow", "path_overflow", timeout=0.1)
    assert not result, "Should fail to put when buffer is at capacity"
    
    # After consuming one item, should be able to put again
    item = shared_buffer.get_shard(timeout=0.1)
    assert item is not None, "Should be able to get an item"
    
    result = shared_buffer.put_shard("fan_new", "path_new", timeout=0.1)
    assert result, "Should be able to put after consuming one item"


def test_put_get_round_trip(shared_buffer):
    """Test that data put into buffer can be retrieved correctly."""
    sender_name = "Test Fan"
    file_path = "/tmp/test_shard.mp4"
    
    # Put a shard
    result = shared_buffer.put_shard(sender_name, file_path, timeout=1.0)
    assert result, "Put should succeed"
    
    # Get the shard back
    item = shared_buffer.get_shard(timeout=1.0)
    assert item is not None, "Should retrieve an item"
    
    retrieved_sender, retrieved_path = item
    assert retrieved_sender == sender_name, "Sender name should match"
    assert retrieved_path == file_path, "File path should match"


def test_get_shard_timeout(shared_buffer):
    """Test that get_shard returns None when buffer is empty."""
    # Buffer is empty, should timeout and return None
    item = shared_buffer.get_shard(timeout=0.1)
    assert item is None, "Should return None when buffer is empty"


def test_qsize(shared_buffer):
    """Test that qsize returns correct buffer occupancy."""
    initial_size = shared_buffer.qsize()
    assert initial_size == 0, "Buffer should start empty"
    
    # Add some items
    shared_buffer.put_shard("fan_1", "path_1", timeout=1.0)
    shared_buffer.put_shard("fan_2", "path_2", timeout=1.0)
    
    size = shared_buffer.qsize()
    assert size == 2, f"Buffer should have 2 items, got {size}"
    
    # Remove one
    shared_buffer.get_shard(timeout=1.0)
    size = shared_buffer.qsize()
    assert size == 1, f"Buffer should have 1 item after consuming one, got {size}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
