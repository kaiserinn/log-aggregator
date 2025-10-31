import asyncio
import dotenv
import httpx
import random
import uuid
from datetime import datetime
import time
from typing import Any
import os

dotenv.load_dotenv()

HOST = os.getenv("AGG_HOST", "localhost")
PORT = os.getenv("AGG_PORT", "7878")

AGGREGATOR_URL = f"http://{HOST}:{PORT}/publish"
TOTAL_EVENTS = 5000
DUPLICATE_PERCENT = 0.2
BATCH_SIZE = 100

topics = ["topic-a", "topic-b", "topic-c", "topic-d"]
sources = ["source-a", "source-b", "source-c", "source-d"]

def generate_events(total_events: int, duplicate_count: int) -> list[dict[str, Any]]:
    unique_event_count = total_events - duplicate_count
    if unique_event_count <= 0:
        raise ValueError("Total events must be greater than duplicate count")
    
    unique_events = []
    
    for _ in range(unique_event_count):
        event = {
            "topic": random.choice(topics),
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "source": random.choice(sources),
            "payload": {
                "user_id": random.randint(1000, 9999),
                "action": "click",
                "value": random.random() * 100
            }
        }
        unique_events.append(event)
        
    duplicate_events = [
        random.choice(unique_events) for _ in range(duplicate_count)
    ]
    
    all_events = unique_events + duplicate_events
    random.shuffle(all_events)
    
    return all_events


async def publish_events(url: str, events: list[dict[str, Any]], batch_size: int):
    total_sent = 0
    total_batches = (len(events) + batch_size - 1) // batch_size
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        for i in range(0, len(events), batch_size):
            batch = events[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            try:
                response = await client.post(url, json=batch)
                
                if 200 <= response.status_code < 300:
                    total_sent += len(batch)
                    print(f"  Batch {batch_num}/{total_batches} sent ({response.status_code}). Total sent: {total_sent}")
                else:
                    print(f"  Batch {batch_num}/{total_batches} FAILED. Status: {response.status_code}, Response: {response.text}")
                    
            except httpx.ConnectError as e:
                print(e)
                print(f"Connection error: Could not connect to {url}. Is the server running?")
                print("Aborting.")
                return 0
            except httpx.ReadTimeout:
                print(f"Read timeout on batch {batch_num}. Server might be overloaded. Skipping batch.")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
                
    return total_sent

async def main():
    duplicate_count = int(TOTAL_EVENTS * DUPLICATE_PERCENT)
    events_to_send = generate_events(TOTAL_EVENTS, duplicate_count)

    print("Test will start in 3 seconds.")
    await asyncio.sleep(3)
    print("Starting test...")
    
    print("Test Parameters:")
    print(f"  - Total Events: {TOTAL_EVENTS}")
    print(f"  - Duplicates:   {duplicate_count} ({DUPLICATE_PERCENT * 100}%)")
    print(f"  - Batch Size:   {BATCH_SIZE}\n")
    
    start_time = time.monotonic()
    total_sent = await publish_events(AGGREGATOR_URL, events_to_send, BATCH_SIZE)
    end_time = time.monotonic()
    duration = end_time - start_time
    
    if total_sent > 0:
        print(f"\nSuccessfully sent {total_sent} events in {duration:.2f} seconds.")
        print(f"Average throughput: {total_sent / duration:.2f} events/second.")
    else:
        print("No events were sent.")

if __name__ == "__main__":
    asyncio.run(main())
