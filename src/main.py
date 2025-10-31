from fastapi import FastAPI, status, Response
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import uvicorn
import asyncio
import time
import json
import os

from consumer import event_consumer
import db
from models import Event

load_dotenv()

start_time = 0
queue: asyncio.Queue | None = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global queue, start_time
    start_time = time.time()
    queue = asyncio.Queue()
    await db.migrate()
    asyncio.create_task(event_consumer(queue))
    yield

app = FastAPI(lifespan=lifespan)

@app.post("/publish")
async def publish_events(events: Event | list[Event], response: Response):
    global queue

    events = events if not(isinstance(events, Event)) else [events]

    if queue is None:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"error": "Service is not ready, queue not initialized"}

    for event in events:
        queue.put_nowait(event)

    return {"message": f"Queued {len(events)} event(s)"}

@app.get("/stats")
async def get_stats():
    uptime = time.time() - start_time
    
    unique_processed = 0
    duplicate_dropped = 0
    topics = []

    async with db.connect() as conn:
        cursor = await conn.execute("SELECT COUNT(*) FROM events")
        result = await cursor.fetchone()
        unique_processed = result[0] if result else 0
        
        cursor = await conn.execute("SELECT duplicate_dropped FROM stats WHERE id = 1")
        result = await cursor.fetchone()
        duplicate_dropped = result[0] if result else 0
        
        cursor = await conn.execute("SELECT DISTINCT topic FROM events")
        rows = await cursor.fetchall()
        topics = [row[0] for row in rows]

    return {
        "received": unique_processed + duplicate_dropped,
        "unique_processed": unique_processed,
        "duplicate_dropped": duplicate_dropped,
        "topics": topics,
        "uptime_seconds": uptime
    }

@app.get("/events", response_model=list[Event])
async def get_events(topic: str):
    events = []
    async with db.connect() as conn:
        cursor = await conn.execute(
            "SELECT * FROM events WHERE topic = ? ORDER BY timestamp DESC",
            (topic,)
        )
        rows = await cursor.fetchall()
        
        for row in rows:
            events.append(Event(
                topic=row[0],
                event_id=row[1],
                timestamp=row[2],
                source=row[3],
                payload=json.loads(row[4])
            ))
    return events

if __name__ == "__main__":
    HOST = os.getenv("HOST", "localhost")
    PORT = os.getenv("PORT", "7878")

    uvicorn.run(app, host=HOST, port=int(PORT))
