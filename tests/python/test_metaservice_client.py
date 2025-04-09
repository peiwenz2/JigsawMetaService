import pytest
from src.python.metaservice_client import MetaServiceClient
import time
import random

@pytest.fixture(scope="module")
def client():
    client = MetaServiceClient()
    client.initialize("./tests/config_test.json")
    yield client
    # Cleanup after tests
    client.batch_delete([f"test_key_{i}" for i in range(5000)])

def test_basic_operations(client):
    # Test single key operations
    client.set("test_key", "test_value")
    assert client.get("test_key") == "test_value"

    # Test delete
    client.delete("test_key")
    assert client.get("test_key") == ""

def test_batch_operations(client):
    # Prepare test data
    test_data = [(f"key_{i}", f"value_{i}") for i in range(100)]

    # Batch write
    client.batch_write(test_data)

    # Batch read
    keys = [k for k, _ in test_data]
    values = client.batch_read(keys)
    assert values == [v for _, v in test_data]

    # Batch delete
    client.batch_delete(keys)
    values = client.batch_read(keys)
    assert all(v == "" for v in values)

def test_performance_batch_operations(client):
    # Generate 5000 test items
    test_items = [(f"perf_key_{i}", f"value_{i}") for i in range(5000)]
    keys = [k for k, _ in test_items]

    # Time batch write
    start = time.time()
    client.batch_write(test_items)
    write_time = time.time() - start
    print(f"\nBatch write 5000 items: {write_time:.3f} seconds")

    # Time batch read
    start = time.time()
    values = client.batch_read(keys)
    read_time = time.time() - start
    print(f"Batch read 5000 items: {read_time:.3f} seconds")
    assert len(values) == 5000

    # Time batch delete
    start = time.time()
    client.batch_delete(keys)
    delete_time = time.time() - start
    print(f"Batch delete 5000 items: {delete_time:.3f} seconds")

def test_advanced_operations(client):
    # Write sample instance data
    client.batch_write([(f"instance_{i}", "data") for i in range(10)])

    # Test scan
    keys = client.scan_keys(pattern="instance_*")
    assert len(keys) >= 10
    assert all(k.startswith("instance_") for k in keys)

    # Test hottest keys (implementation specific)
    hot_keys = client.get_hottest_keys(prefix="instance_*")
    assert len(hot_keys) > 0

if __name__ == "__main__":
    # Manual test execution
    client = MetaServiceClient()
    client.initialize("./tests/config_test.json")

    try:
        # Basic operations
        client.set("demo_key", "demo_value")
        print("Get result:", client.get("demo_key"))

        # Performance test
        test_performance_batch_operations(client)

    finally:
        client.delete("demo_key")
        client.batch_delete([f"perf_key_{i}" for i in range(5000)])