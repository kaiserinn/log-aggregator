import asyncio
import httpx
import time
import uuid
from datetime import datetime
import os
import random
import sys

# --- Configuration ---

# Get host/port from environment or use defaults
HOST = os.getenv("AGG_HOST", "localhost")
PORT = os.getenv("AGG_PORT", "7878")
AGGREGATOR_URL = f"http://{HOST}:{PORT}/publish"

# Number of events to send for each test
# Using a smaller number for single events to avoid a very long test
NUM_EVENTS_SINGLE_TEST = 500
NUM_EVENTS_BATCH_TEST = 5000
BATCH_SIZE = 100  # Number of events per batch

# --- ---

def create_event_payload() -> dict:
    """Generates a single random event payload."""
    return {
        "topic": random.choice(["system", "auth", "payment", "logs"]),
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now().isoformat(),
        "source": "perf_test_script",
        "payload": {"value": random.random(), "user": random.randint(1, 100)}
    }

async def check_server_ready(client: httpx.AsyncClient) -> bool:
    """Checks if the /stats endpoint is reachable."""
    try:
        stats_url = f"http://{HOST}:{PORT}/stats"
        resp = await client.get(stats_url, timeout=5.0)
        resp.raise_for_status()
        print(f"✅ Server is ready at {HOST}:{PORT}")
        return True
    except httpx.RequestError as e:
        print(f"❌ Server not reachable at {HOST}:{PORT}. ({e})")
        print("Please ensure the aggregator container is running.")
        return False

async def test_single_event_performance(client: httpx.AsyncClient, num_events: int):
    """
    Sends N events one by one and measures latency for each.
    """
    print(f"\n--- Testing Single Event Performance ({num_events} events) ---")
    latencies = []
    total_start_time = time.monotonic()
    success_count = 0
    
    for i in range(num_events):
        event = create_event_payload()
        start_time = time.monotonic()
        try:
            resp = await client.post(AGGREGATOR_URL, json=event)
            resp.raise_for_status()  # Check for HTTP errors
            end_time = time.monotonic()
            latencies.append(end_time - start_time)
            success_count += 1
        except httpx.RequestError as e:
            print(f"Request failed for event {i}: {e}")
            continue
    
    total_end_time = time.monotonic()
    total_duration = total_end_time - total_start_time
    
    if not latencies:
        print("No successful requests.")
        print("-" * 50)
        return

    avg_latency_ms = (sum(latencies) / len(latencies)) * 1000
    throughput = success_count / total_duration
    
    print("\n--- Single Event Results ---")
    print(f"Total time: {total_duration:.4f} s")
    print(f"Successful requests: {success_count} / {num_events}")
    print(f"Average latency (per event): {avg_latency_ms:.2f} ms")
    print(f"Throughput (events/sec): {throughput:.2f}")
    print("-" * 50)

async def test_batched_event_performance(client: httpx.AsyncClient, num_events: int, batch_size: int):
    """
    Sends N events in batches of size B and measures latency per batch.
    """
    print(f"\n--- Testing Batched Event Performance ({num_events} events, size {batch_size}) ---")
    events = [create_event_payload() for _ in range(num_events)]
    num_batches = (num_events + batch_size - 1) // batch_size
    batch_latencies = []  # Will store latency *per batch*
    success_count = 0
    
    total_start_time = time.monotonic()
    
    for i in range(0, num_events, batch_size):
        batch = events[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        start_time = time.monotonic()
        try:
            resp = await client.post(AGGREGATOR_URL, json=batch)
            resp.raise_for_status()
            end_time = time.monotonic()
            batch_latencies.append(end_time - start_time)
            success_count += 1
        except httpx.RequestError as e:
            print(f"Request failed for batch {batch_num}: {e}")
            continue
    
    total_end_time = time.monotonic()
    total_duration = total_end_time - total_start_time
    
    if not batch_latencies:
        print("No successful batches.")
        print("-" * 50)
        return

    avg_batch_latency_ms = (sum(batch_latencies) / len(batch_latencies)) * 1000
    # Throughput is total events sent / total time
    total_successful_events = success_count * batch_size
    throughput = total_successful_events / total_duration
    
    print("\n--- Batched Event Results ---")
    print(f"Total time: {total_duration:.4f} s")
    print(f"Successful batches: {success_count} / {num_batches}")
    print(f"Average latency (per batch): {avg_batch_latency_ms:.2f} ms")
    print(f"Throughput (events/sec): {throughput:.2f}")
    print("-" * 50)

async def main():
    print(f"Starting performance test against {AGGREGATOR_URL}...")
    
    # Use a persistent client with a generous timeout
    async with httpx.AsyncClient(timeout=30.0) as client:
        
        if not await check_server_ready(client):
            sys.exit(1)
        
        # 1. Test single event performance
        await test_single_event_performance(client, num_events=NUM_EVENTS_SINGLE_TEST)
        
        print("\nWaiting 2s for server to settle...\n")
        await asyncio.sleep(2) 
        
        # 2. Test batched event performance
        await test_batched_event_performance(client, num_events=NUM_EVENTS_BATCH_TEST, batch_size=BATCH_SIZE)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTest interrupted by user.")
