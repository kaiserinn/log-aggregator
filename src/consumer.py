import asyncio
import json

import db
from models import Event

async def process_event(event: Event):
    is_new_event = False 

    async with db.connect() as conn:
        cursor = await conn.execute(
            "INSERT INTO events VALUES(?,?,?,?,?) ON CONFLICT DO NOTHING", 
            (event.topic, str(event.event_id), event.timestamp, event.source, json.dumps(event.payload))
        )
        
        if cursor.rowcount > 0:
            is_new_event = True
        else:
            print(f"Duplicate {(event.topic, event.event_id)}")
            await conn.execute("""
                INSERT INTO stats(id,duplicate_dropped) 
                VALUES(1, 1) 
                ON CONFLICT(id) DO UPDATE SET duplicate_dropped=duplicate_dropped+1
            """)
        
        await conn.commit()
        await cursor.close()

    if is_new_event:
        asyncio.create_task(consume_event(event))

async def event_consumer(queue: asyncio.Queue):
    print("Event consumer started, waiting for events...")

    while True:
        try:
            event = await queue.get()
            await process_event(event)

        except asyncio.CancelledError:
            print("Consumer task received cancellation request.")
            break

        except Exception as e:
            print(f"Error processing event: {e}")

async def consume_event(event: Event):
    print(event)
