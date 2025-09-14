import json, time, random as r
from kafka import KafkaProducer
from cdr_api import generate_cdr

BROKER = "localhost:29092"
TOPIC = "telecom.cdr"

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

if __name__=="__main__":
    print(f"Producing CDR events to {TOPIC}...")
    try:
        while True:
            event = generate_cdr()
            producer.send(TOPIC, value=event)
            print(f"Sent: {event}")
            time.sleep(r.uniform(0.2, 1.5))
    except KeyboardInterrupt:
        print("Stopping producer.")
    finally:
        producer.flush()
        producer.close()