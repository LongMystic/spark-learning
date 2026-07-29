"""
Tiny Kafka producer for the Structured Streaming lessons (Days 30-31).

Emits synthetic JSON transaction events to a Kafka topic so the streaming
exercises have something to consume. Requires the `kafka` service on minikube:

    kubectl apply -f environment/k8s/06-kafka.yaml
    kubectl -n spark-jobs port-forward svc/kafka 9092:9092 &
    Run via Kubernetes (see environment/README.md)

Each message value is JSON:
    {"txn_id": 1, "customer_id": 42, "amount": 12.50, "event_time": "2026-07-22T10:00:01"}
"""

import argparse
import json
import time
from datetime import datetime, timezone

try:
    from kafka import KafkaProducer
except ImportError:
    raise SystemExit("pip install kafka-python  (see environment/requirements.txt)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="transactions")
    ap.add_argument("--rate", type=int, default=10, help="messages per second")
    ap.add_argument("--customers", type=int, default=100)
    ap.add_argument("--count", type=int, default=0, help="0 = run forever")
    args = ap.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    sent = 0
    interval = 1.0 / max(args.rate, 1)
    print(f"Producing to {args.bootstrap} topic '{args.topic}' at ~{args.rate} msg/s ...")
    try:
        while args.count == 0 or sent < args.count:
            event = {
                "txn_id": sent,
                "customer_id": sent % args.customers,
                "amount": round((sent % 200) + 1 + (sent % 7) / 10, 2),
                "event_time": datetime.now(timezone.utc).isoformat(),
            }
            producer.send(args.topic, event)
            sent += 1
            if sent % args.rate == 0:
                producer.flush()
                print(f"  sent {sent} events")
            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\nStopped after {sent} events.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
