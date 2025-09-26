import uuid, random as r
from datetime import datetime, timedelta

SUBSCRIBERS_IDS = list(range(1, 501))
TOWER_IDS = list(range(1, 161))
CALL_TYPES = ["voice", "sms", "data"]

def generate_cdr():
    call_id = str(uuid.uuid4())
    subscribers_id = r.choice(SUBSCRIBERS_IDS)
    tower_id = r.choice(TOWER_IDS)
    call_type = r.choice(CALL_TYPES)
    start = datetime.utcnow()
    duration = r.randint(1, 300)    #1s-5min
    end = start + timedelta(seconds=duration)
    
    return {
        "call_id": call_id,
        "subscribers_id": subscribers_id,
        "call_start": start.isoformat(),
        "call_end": end.isoformat(),
        "duration_sec": duration,
        "call_type": call_type,
        "tower_id": tower_id
    }
