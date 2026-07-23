# Stage 3: Real-Time Streaming Layer

## Tasks

- Design Kafka topics for the company’s streaming data sources
- Build a producer that simulates real-time events from your scenario
- Implement Spark Structured Streaming consumers
- Apply windowed aggregations and/or real-time alerting logic
- Decide and justify your architecture: Lambda (batch + stream) or Kappa (stream only)

## Implementation

### Running Kafka producer

- Create a Kafka topic in `kafka` container to store messages: `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9094 --create --if-not-exists --topic transactions --partitions 6 --replication-factor 1 --config retention.ms=-1 --config segment.bytes=104857600`
- Run a Kafka producer (Python app) to simulate real-time events from transaction data: `docker compose exec kafka-producer python /opt/producers/transaction_producer.py --rate 5000`

    ```bash
    Sent 1000 messages in 0.2s (4904 msg/s)
    Sent 2000 messages in 0.4s (4833 msg/s)
    Sent 3000 messages in 0.6s (4994 msg/s)
    Sent 4000 messages in 0.8s (4989 msg/s)
    Sent 5000 messages in 1.0s (4878 msg/s)
    Sent 6000 messages in 1.3s (4760 msg/s)
    Sent 7000 messages in 1.4s (4985 msg/s)
    Sent 8000 messages in 1.6s (4997 msg/s)
    Sent 9000 messages in 1.8s (4993 msg/s)
    Sent 10000 messages in 2.0s (4998 msg/s)
    ...
    ```
- Verification

    - Topic exists with the right config

        ```bash
        docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9094 --describe --topic transactions
        # Expected: Topic: transactions, PartitionCount: 6, retention.ms=-1, segment.bytes=104857600
        ```
    
    - Topic has ~1M messages, distributed across all 6 partitions

        ```bash
        docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9094 --topic transactions
        # transactions:0:165639
        # transactions:1:170428
        # transactions:2:164850
        # transactions:3:165588
        # transactions:4:167799
        # transactions:5:165696
        ```
    - Sample messages (keyed, JSON-valued)

        ```bash
        docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9094 --topic transactions --from-beginning --max-messages 3 --property print.key=true --property key.separator=" | "
        # CARD-052186 | {"txn_id": "TXN-0000007", "timestamp": "2025-02-10 11:26:56", "card_id": "CARD-052186", "merchant_id": "MERCH-01653", "amount": "510.94", "currency": "USD", "merchant_category": "restaurant", "country": "CN", "channel": "in_store", "is_international": "true"}
        # CARD-038134 | {"txn_id": "TXN-0000009", "timestamp": "2025-01-24 03:38:30", "card_id": "CARD-038134", "merchant_id": "MERCH-09579", "amount": "635.04", "currency": "USD", "merchant_category": "retail", "country": "US", "channel": "atm", "is_international": "false"}
        # CARD-064095 | {"txn_id": "TXN-0000020", "timestamp": "2025-05-06 21:32:09", "card_id": "CARD-064095", "merchant_id": "MERCH-09761", "amount": "382.58", "currency": "USD", "merchant_category": "travel", "country": "US", "channel": "online", "is_international": "false"}
        ```

### Spark Structured Streaming (Kafka consumer)

- Create the Kafka topics

    ```bash
    for t in transactions-scored fraud-alerts; do
    docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9094 \
        --create --if-not-exists --topic "$t" --partitions 6 --replication-factor 1 \
        --config retention.ms=-1
    done

    for t in customer-features merchant-directory; do
    docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9094 \
        --create --if-not-exists --topic "$t" --partitions 6 --replication-factor 1 \
        --config cleanup.policy=compact # enable log compaction, keep only the latest value for key
    done
    ```

- Submit Spark `publish_customer_features` and `publish_merchant_directory` jobs

    ```bash
    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
        /opt/spark/work-dir/jobs/publish/publish_customer_features.py
    # expect: published 100000 card feature rows to topic 'customer-features'

    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
        /opt/spark/work-dir/jobs/publish/publish_merchant_directory.py
    # expect: published 10000 merchant rows to topic 'merchant-directory'
    ```

- Submit Spark Structured Streaming `stream_score.py` job: `docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 /opt/spark/work-dir/jobs/kafka_consumer/stream_score.py`

- Verification

    ```bash
    docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
        --bootstrap-server kafka:9094 --topic transactions-scored
    transactions-scored:0:167020
    transactions-scored:1:166928
    transactions-scored:2:167125
    transactions-scored:3:166155
    transactions-scored:4:166740
    transactions-scored:5:166032
    ```

    ```bash
    docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
        --bootstrap-server kafka:9094 --topic fraud-alerts
    fraud-alerts:0:5894
    fraud-alerts:1:5805
    fraud-alerts:2:5813
    fraud-alerts:3:5756
    fraud-alerts:4:5915
    fraud-alerts:5:5869
    ```

    ```bash
    docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server kafka:9094 --topic transactions-scored \
        --from-beginning --max-messages 3

    {"txn_id":"TXN-0426887","card_id":"CARD-025694","event_time":"2025-03-06T20:28:58.000Z","merchant_id":"MERCH-00072","amount":164.22,"country":"UK","channel":"online","is_international":true,"velocity_count":1,"merchant_risk_score":2,"rule_high_amount":false,"rule_velocity":false,"rule_international_mismatch":false,"rule_high_risk_merchant":false,"risk_score":0,"predicted_fraud":false,"recommended_action":"approve"}
    {"txn_id":"TXN-0068076","card_id":"CARD-044072","event_time":"2025-01-13T04:44:52.000Z","merchant_id":"MERCH-00072","amount":710.29,"country":"UK","channel":"in_store","is_international":true,"velocity_count":1,"merchant_risk_score":2,"rule_high_amount":true,"rule_velocity":false,"rule_international_mismatch":false,"rule_high_risk_merchant":false,"risk_score":1,"predicted_fraud":false,"recommended_action":"approve"}
    {"txn_id":"TXN-0230954","card_id":"CARD-061353","event_time":"2025-05-04T08:07:14.000Z","merchant_id":"MERCH-00072","amount":1068.6,"country":"UK","channel":"contactless","is_international":true,"velocity_count":1,"merchant_risk_score":2,"rule_high_amount":true,"rule_velocity":false,"rule_international_mismatch":false,"rule_high_risk_merchant":false,"risk_score":1,"predicted_fraud":false,"recommended_action":"approve"}
    ```

    ```bash
    docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server kafka:9094 --topic fraud-alerts \
        --from-beginning --max-messages 3

    {"txn_id":"TXN-0657182","card_id":"CARD-004434","event_time":"2025-04-12T07:43:01.000Z","amount":821.91,"risk_score":2,"triggered_rules":"high_amount,high_risk_merchant","recommended_action":"review"}
    {"txn_id":"TXN-0316394","card_id":"CARD-004729","event_time":"2025-02-01T12:20:01.000Z","amount":542.22,"risk_score":2,"triggered_rules":"high_amount,high_risk_merchant","recommended_action":"review"}
    {"txn_id":"TXN-0996763","card_id":"CARD-075893","event_time":"2025-01-30T20:41:37.000Z","amount":298.01,"risk_score":2,"triggered_rules":"high_amount,high_risk_merchant","recommended_action":"review"}
    ```



Content in `docker compose exec -T namenode hdfs dfs -cat /checkpoints/finpulse-stream-score/offsets/0`

```bash
{"batchWatermarkMs":0,"batchTimestampMs":1784280404424,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider","spark.sql.streaming.stateStore.rocksdb.formatVersion":"5","spark.sql.streaming.stateStore.encodingFormat":"unsaferow","spark.sql.streaming.statefulOperator.useStrictDistribution":"true","spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.aggregation.stateFormatVersion":"2","spark.sql.shuffle.partitions":"200","spark.sql.streaming.join.stateFormatVersion":"2","spark.sql.streaming.stateStore.compression.codec":"lz4","spark.sql.optimizer.pruneFiltersCanPruneStreamingSubplan":"false"}}
{"transactions":{"0":165639,"1":170428,"2":164850,"3":165588,"4":167799,"5":165696}}
```