from pydantic import BaseModel
from datetime import datetime

class Event(BaseModel):
    topic: str
    event_id: str
    timestamp: datetime
    source: str
    payload: dict
