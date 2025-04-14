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

    # Test GetAliveInstanceList
    client.set("alive_instance_list", "instance1,instance2,instance3")
    assert len(client.get_alive_instance_list()) == 3
    client.delete("alive_instance_list")
    assert client.get("alive_instance_list") == ""

def test_zset_operations(client):
    zset_key = "test_zset"
    client.delete(zset_key) # clean if exist

    # Test 1: write a single member
    client.zwrite(zset_key, "member1", 10.5)
    assert client.zreadrange(zset_key, 0) == [("member1", 10.5)]

    # Test 2: write multiple members and get all
    client.zwrite(zset_key, "member2", 20.0)
    client.zwrite(zset_key, "member3", 5.5)
    assert client.zreadrange(zset_key, 0) == [
        ("member3", 5.5),
        ("member1", 10.5),
        ("member2", 20.0)
    ]

    # Test 3: check topN
    assert client.zreadrange(zset_key, 2) == [
        ("member2", 20.0),
        ("member1", 10.5)
    ]

    # Test 4: check updating member score
    client.zwrite(zset_key, "member1", 25.0)  # 更新分数
    assert client.zreadrange(zset_key, 0) == [
        ("member3", 5.5),
        ("member2", 20.0),
        ("member1", 25.0),
    ]

    # Test 5: check delete zset
    client.delete(zset_key) # delete the whole zset
    assert client.zreadrange(zset_key, 0) == []

    # Test 6: if score same
    client.zwrite(zset_key, "memberB", 15.0)
    client.zwrite(zset_key, "memberA", 15.0)
    assert client.zreadrange(zset_key, 0) == [
        ("memberA", 15.0),
        ("memberB", 15.0)
    ]
    assert len(client.zreadrange(zset_key, 0)) == 2

    client.zdelete(zset_key, "memberB")
    assert len(client.zreadrange(zset_key, 0)) == 1

    client.delete(zset_key)

def test_basic_batch_zwrite(client):
    """测试基础批量写入功能"""
    entries = [
        ("test_key_1", "member1", 10.5),
        ("test_key_1", "member2", 20.0),
        ("test_key_2", "memberA", 15.0),
        ("test_key_2", "memberB", 5.5),
    ]

    client.batch_zwrite(entries)

    results = client.zreadrange("test_key_1", 0)
    assert len(results) == 2
    assert ("member2", 20.0) in results
    assert ("member1", 10.5) in results

    results = client.zreadrange("test_key_2", 0)
    assert results == [("memberB", 5.5), ("memberA", 15.0)]

    large_entries = [
        ("test_key_3", f"member{i}", float(i))
        for i in range(1000)
    ]

    start = time.time()
    client.batch_zwrite(large_entries)
    write_time = time.time() - start
    print(f"\nBatch write zset 1000 members: {write_time:.3f} seconds")

    results = client.zreadrange("test_key_3", 0)
    assert len(results) == 1000

    assert results[0][0] == "member0"
    assert results[-1][0] == "member999"

    client.delete("test_key_1")
    client.delete("test_key_2")
    client.delete("test_key_3")

    large_keys_entries = []
    large_keys = []
    for i in range(1000):
        large_keys_entries.append((f"test_key_{i}", "member", 1.0))
        large_keys.append(f"test_key_{i}")
    start = time.time()
    client.batch_zwrite(large_entries)
    write_time = time.time() - start
    print(f"\nBatch write zset 1000 keys: {write_time:.3f} seconds")
    start = time.time()
    results = client.batch_zread(large_keys)
    write_time = time.time() - start
    print(f"\nBatch read zset 1000 keys: {write_time:.3f} seconds")
    assert len(results) == 1000

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