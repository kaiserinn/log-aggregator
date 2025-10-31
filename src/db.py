import aiosqlite
import os

db_path = os.getenv("DB_PATH", "./data/store");

def connect():
    global db_path
    return aiosqlite.connect(db_path)

async def migrate():
    async with connect() as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS events (
                topic TEXT,
                event_id TEXT,
                timestamp TIMESTAMP,
                source TEXT,
                payload JSON,
                PRIMARY KEY (topic, event_id)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS stats (
                id INTEGER PRIMARY KEY,
                duplicate_dropped INT
            )
        """)
