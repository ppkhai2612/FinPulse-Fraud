"""Kafka Producer for Transactions

Streaming transaction data as messages into Kafka topic
"""
import argparse
import json
import gzip
import csv
import time

from kafka import KafkaProducer


CSV_PATH = "/opt/data/transactions.csv.gz" # .csv.gz file to read
BOOTSTRAP = "kafka:9094" # INTERNAL listener
TOPIC = "transactions" # topic name


def parse_args() -> argparse.Namespace:
    """Parsing this producer's argument strings"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--rate",
        default=200,
        type=int,
        help="The number of messages per second (default: 200)",
    )
    parser.add_argument(
        "--limit",
        default=None,
        type=int,
        help="Stop after N messages (default: send all transactions)"
    )
    return parser.parse_args()


def main():
    """Main logic for Kafka Producer

        - Handle command-line arguments (--rate and --limit)
        - Initialize a KafkaProducer with acks='all', which means wait for the full set of in-sync replicas to write the record
        - Read file and sent each row as a Kafka message to predefined topic

            - Since send() is asynchronous, a successful return from the method does not mean the broker has already received the message
            - The message may still be buffered, in transit, or waiting for an ACK
            - flush() waits until all pending messages have been fully processed, ensuring there are no outstanding sends before the producer is closed or the application terminates
    """
    args = parse_args()
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks='all'
    )

    sent = 0 # keep the no. messages sent to the topic for each loop
    start = time.monotonic()
    with gzip.open(CSV_PATH, mode="rt", encoding="utf-8") as csv_file:
        for row in csv.DictReader(csv_file):

            if args.limit is not None and sent >= args.limit: # if no. messages reaches the limit
                break
        
            producer.send(TOPIC, key=row["card_id"], value=row)
            sent += 1

            # handle the sending rate
            target = start + sent / args.rate
            now = time.monotonic()
            if now < target: # producer is sending at a rate exceeding the rate limit
                time.sleep(target - now)

            if sent % 1000 == 0: # if 1000 messages sent
                elapsed = time.monotonic() - start
                print(f"Sent {sent} messages in {elapsed:.1f}s ({sent / elapsed:.0f} msg/s)")

    producer.flush()
    producer.close()
    elapsed = time.monotonic() - start
    print(f"DONE. Sent all {sent} messages to topic '{TOPIC}' in {elapsed:.1f}s ({sent / elapsed:.0f} msg/s)")


if __name__ == "__main__":
    main()