import httpx
import os
import asyncio
from src.publisher.publish import generate_events
from time import time

BATCH_SIZE = 100
HOST = os.getenv("AGG_HOST", "localhost")
PORT = os.getenv("AGG_PORT", "7878")
URL = f"http://{HOST}:{PORT}/publish"

async def main():
    events = generate_events(5000, 0.2)

    total_sent = 0
    total_batches = (len(events) + BATCH_SIZE - 1) // BATCH_SIZE
    
    start_time = time()

    async with httpx.AsyncClient(timeout=30.0) as client:
        for i in range(0, len(events), BATCH_SIZE):
            batch = events[i:i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            
            await client.post(URL, json=batch)

            total_sent += len(batch)
            print(f"  Batch {batch_num}/{total_batches} sent. Total sent: {total_sent}")

    end_time = time()
    duration = end_time - start_time

    if total_sent > 0:
        print(f"\nSuccessfully sent {total_sent} events in {duration:.2f} seconds.")
        print(f"Average throughput: {total_sent / duration:.2f} events/second.")
    else:
        print("No events were sent.")

if __name__ == "__main__":
    asyncio.run(main())
