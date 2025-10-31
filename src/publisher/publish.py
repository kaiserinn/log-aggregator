import random
import uuid
import math
import datetime
from string import ascii_letters

topics = ["topic-a", "topic-b", "topic-c", "topic-d"]
sources = ["source-a", "source-b", "source-c", "source-d"]

def generate_events(n = 1, dup_rate = 0.0):
    n_unique = math.floor(n * (1 - dup_rate))
    events = []
    for _ in range(n_unique):
        events.append({
            "topic": random.choice(topics),
            "event_id": str(uuid.uuid4()),
            "source": random.choice(sources),
            "timestamp": datetime.datetime.now().isoformat(),
            "payload": {
                "message": "".join(random.choice(
                    ascii_letters
                ) for _ in range(random.randint(10,100)))
            }
        })
    events = events + [
        random.choice(events)
        for _ in range(math.floor(n * dup_rate))
    ]
    random.shuffle(events)
    return events
