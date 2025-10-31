import pytest
import httpx
import asyncio
import subprocess
import sys
import os
import time
import datetime
import dotenv
from uuid import uuid4
from pydantic import ValidationError
from src.models import Event

dotenv.load_dotenv()

PORT = os.getenv("ARG_PORT", "7878")
HOST = os.getenv("ARG_HOST", "localhost")
BASE_URL = f"http://{HOST}:{int(PORT)}"

@pytest.fixture(scope="function")
async def test_server():
    db_path = f"./test_db_{uuid4()}.db"
    
    env = os.environ.copy()
    env["DB_PATH"] = db_path
    env["PORT"] = PORT

    process = subprocess.Popen(
        [sys.executable, './src/main.py'], 
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    await asyncio.sleep(2.0) 

    is_alive = False
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{BASE_URL}/stats")
            is_alive = resp.status_code == 200
    except httpx.ConnectError:
        pass

    if not is_alive:
        process.terminate()
        process.wait()
        
        stderr_output = "No stderr output captured."
        if process.stderr:
            stderr_output = process.stderr.read().decode()
            
        pytest.fail(f"Server gagal dimulai pada {BASE_URL}. Error: {stderr_output}")

    yield BASE_URL
    
    process.terminate()
    process.wait()
    
    if os.path.exists(db_path):
        os.remove(db_path)


@pytest.fixture(scope="function")
def restartable_server():
    db_path = f"./test_db_{uuid4()}.db"
    env = os.environ.copy()
    env["DB_PATH"] = db_path
    env["PORT"] = PORT

    process = subprocess.Popen(
        [sys.executable, './src/main.py'], 
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    time.sleep(2.0) 

    server_state = {"process": process, "env": env, "url": BASE_URL}

    yield server_state
    
    if server_state["process"]:
        server_state["process"].terminate()
        server_state["process"].wait()
    
    if os.path.exists(db_path):
        os.remove(db_path)


def test_event_schema_validation_valid():
    data = {
        "topic": "foo",
        "event_id": str(uuid4()),
        "timestamp": datetime.datetime.now(),
        "source": "somewhere",
        "payload": {},
    }
    try:
        Event(**data)
    except ValidationError as e:
        pytest.fail(f"Valid data failed validation: {e}")

def test_event_schema_validation_missing_field():
    data = {
        "event_id": str(uuid4()),
        "timestamp": datetime.datetime.now(),
        "source": "somewhere",
        "payload": {},
    }
    with pytest.raises(ValidationError) as e:
        Event(**data)
    
    errors = e.value.errors()
    assert len(errors) == 1
    assert errors[0]["loc"] == ("topic",)
    assert errors[0]["type"] == "missing"

def test_event_schema_validation_invalid_type_timestamp():
    data = {
        "topic": "foo",
        "event_id": str(uuid4()),
        "timestamp": "ini bukan tanggal!",
        "source": "somewhere",
        "payload": {},
    }
    with pytest.raises(ValidationError) as e:
        Event(**data)
    
    errors = e.value.errors()
    assert len(errors) == 1
    assert errors[0]["loc"] == ("timestamp",)
    assert "datetime" in errors[0]["type"]

def test_event_schema_validation_invalid_type_payload():
    data = {
        "topic": "foo",
        "event_id": str(uuid4()),
        "timestamp": datetime.datetime.now(),
        "source": "somewhere",
        "payload": "ini bukan dict",
    }
    with pytest.raises(ValidationError) as e:
        Event(**data)
    
    errors = e.value.errors()
    assert len(errors) == 1
    assert errors[0]["loc"] == ("payload",)
    assert errors[0]["type"] == "dict_type"


@pytest.mark.asyncio
async def test_simple_deduplication(test_server):
    publish_url = f"{test_server}/publish"
    stats_url = f"{test_server}/stats"
    
    event_id = str(uuid4())
    event_data = {
        "topic": "foo",
        "event_id": event_id,
        "timestamp": datetime.datetime.now().isoformat(),
        "source": "test-dedup",
        "payload": {"data": "A"},
    }

    async with httpx.AsyncClient() as client:
        res1 = await client.post(publish_url, json=event_data)
        assert res1.status_code == 200

        event_data["timestamp"] = datetime.datetime.now().isoformat()

        res2 = await client.post(publish_url, json=event_data)
        assert res2.status_code == 200

        await asyncio.sleep(0.5)

        res_stats = await client.get(stats_url)
        stats_data = res_stats.json()

        assert stats_data["received"] == 2
        assert stats_data["unique_processed"] == 1
        assert stats_data["duplicate_dropped"] == 1


@pytest.mark.asyncio
async def test_persistent_deduplication(restartable_server):
    publish_url = f"{restartable_server['url']}/publish"
    stats_url = f"{restartable_server['url']}/stats"
    
    event_id = str(uuid4())
    event_data = {
        "topic": "persistent-topic",
        "event_id": event_id,
        "timestamp": datetime.datetime.now().isoformat(),
        "source": "test-persist",
        "payload": {"data": "B"},
    }

    async with httpx.AsyncClient() as client:
        res1 = await client.post(publish_url, json=event_data)
        assert res1.status_code == 200
        
        await asyncio.sleep(0.5)

        res_stats1 = await client.get(stats_url)
        stats_data1 = res_stats1.json()
        assert stats_data1["unique_processed"] == 1
        assert stats_data1["duplicate_dropped"] == 0

    print("\nRestarting server untuk tes persistensi...")
    
    process_to_stop = restartable_server["process"]
    process_to_stop.terminate()
    process_to_stop.wait()

    new_process = subprocess.Popen(
        [sys.executable, './src/main.py'], 
        env=restartable_server["env"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    restartable_server["process"] = new_process
    
    await asyncio.sleep(2.0) 
    print("Server di-restart.")

    async with httpx.AsyncClient() as client:
        event_data["timestamp"] = datetime.datetime.now().isoformat()
        res2 = await client.post(publish_url, json=event_data)
        assert res2.status_code == 200

        await asyncio.sleep(0.5)

        res_stats2 = await client.get(stats_url)
        stats_data2 = res_stats2.json()

        assert stats_data2["unique_processed"] == 1
        assert stats_data2["duplicate_dropped"] == 1
        assert stats_data2["received"] == 2


@pytest.mark.asyncio
async def test_consistent_stats_and_events(test_server):
    publish_url = f"{test_server}/publish"
    stats_url = f"{test_server}/stats"
    events_url = f"{test_server}/events"
    
    e1 = Event(topic="foo", event_id=str(uuid4()), timestamp=datetime.datetime.now(), source="src1", payload={"data": 1})
    e2 = Event(topic="bar", event_id=str(uuid4()), timestamp=datetime.datetime.now(), source="src2", payload={"data": 2})
    e3 = Event(topic="baz", event_id=str(uuid4()), timestamp=datetime.datetime.now(), source="src3", payload={"data": 3})

    d1 = Event(topic="foo", event_id=e1.event_id, timestamp=datetime.datetime.now(), source="src-dup1", payload={"data": 1.1})
    d2 = Event(topic="bar", event_id=e2.event_id, timestamp=datetime.datetime.now(), source="src-dup2", payload={"data": 2.2})

    events_batch = [
        e1.model_dump(mode="json"),
        e2.model_dump(mode="json"),
        e3.model_dump(mode="json"),
        d1.model_dump(mode="json"),
        d2.model_dump(mode="json"),
    ]

    async with httpx.AsyncClient() as client:
        res_pub = await client.post(publish_url, json=events_batch)
        assert res_pub.status_code == 200
        assert res_pub.json()["message"] == "Queued 5 event(s)"

        await asyncio.sleep(1.0)

        res_stats = await client.get(stats_url)
        stats_data = res_stats.json()

        assert stats_data["received"] == 5
        assert stats_data["unique_processed"] == 3
        assert stats_data["duplicate_dropped"] == 2
        assert set(stats_data["topics"]) == {"foo", "bar", "baz"}

        
        res_foo = await client.get(f"{events_url}?topic=foo")
        data_foo = res_foo.json()
        assert len(data_foo) == 1
        assert data_foo[0]["event_id"] == e1.event_id
        assert data_foo[0]["payload"] == e1.payload 

        res_bar = await client.get(f"{events_url}?topic=bar")
        data_bar = res_bar.json()
        assert len(data_bar) == 1
        assert data_bar[0]["event_id"] == e2.event_id
        
        res_baz = await client.get(f"{events_url}?topic=baz")
        data_baz = res_baz.json()
        assert len(data_baz) == 1
        assert data_baz[0]["event_id"] == e3.event_id


def _generate_events_json(count: int) -> list[dict]:
    events = []
    for i in range(count):
        events.append({
            "topic": "perf-test",
            "event_id": str(uuid4()),
            "timestamp": datetime.datetime.now().isoformat(),
            "source": f"generator-{i}",
            "payload": {"index": i}
        })
    return events

@pytest.mark.asyncio
async def test_batch_publish_time_100(test_server):
    publish_url = f"{test_server}/publish"
    
    events_batch = _generate_events_json(100)
    
    async with httpx.AsyncClient() as client:
        start_time = time.time()
        
        res_pub = await client.post(publish_url, json=events_batch)
        
        elapsed_time = time.time() - start_time
        
        assert res_pub.status_code == 200
        
        print(f"\nWaktu publish 100 event: {elapsed_time:.4f} detik")
        assert elapsed_time <= 0.5, "Publishing 100 events took too long"
