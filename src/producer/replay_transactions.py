"""Replay data/transactions.csv.gz into Kafka topic 'transaction'.

Each CSV row becomes one Kafka message:
    - key   = card_id (bytes) - same card -> same partition, ordering preserved
    - value = JSON-encoded row (bytes)

Run from inside the 'producer' container:
    docker compose exec producer python /opt/producer/replay_transactions.py
"""

import argparse
import json
import gzip
import csv
import time

from kafka import KafkaProducer


CSV_PATH = "/data/transactions.csv.gz" # bind-mounted from ./data
BOOTSTRAP = "kafka:9094" # INTERNAL listener (we're inside the network)
TOPIC = "transactions"


def parse_args() -> argparse.Namespace:
    """Parsing this producer's argument strings"""
    p = argparse.ArgumentParser()
    p.add_argument(
        "--rate",
        default=200,
        type=int,
        help="max messages per second (default: 200)",
    )
    p.add_argument(
        "--limit",
        default=None,
        type=int,
        help="stop after N messages (default: send all rows)"
    )
    return p.parse_args()


def main():
    args = parse_args()
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks='all',
    )

    sent = 0 # keep the no. messages sent to the topic for each loop
    start = time.monotonic()
    with gzip.open(CSV_PATH, mode="rt", encoding="utf-8") as csv_file:
        for row in csv.DictReader(csv_file):

            if args.limit is not None and sent >= args.limit: # if no. messages reaches the limit
                break
        
            producer.send(TOPIC, key=row["card_id"], value=row) # key card_id for partition
            sent += 1

            # Pace: sleep until 'sent' is back on the rate budget
            target = start + sent / args.rate
            now = time.monotonic()
            if now < target: # producer is sending at a rate exceeding the rate limit
                time.sleep(target - now)

            if sent % 1000 == 0: # if 1000 messages sent
                elapsed = time.monotonic() - start
                print(f"Sent {sent} messages in {elapsed:.1f}s ({sent / elapsed:.0f} msg/s)")

    producer.flush() # wait for all buffered messages to be sent
    producer.close()
    elapsed = time.monotonic() - start
    print(f"DONE. Sent all {sent} messages to topic '{TOPIC}' in {elapsed:.1f}s ({sent / elapsed:.0f} msg/s)")


if __name__ == "__main__":
    main()