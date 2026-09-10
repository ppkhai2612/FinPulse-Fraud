# Kafka

**Source of truth for the `transactions` fact stream**. Transactions never land in HDFS - the producer publishes `transactions.csv.gz` directly to Kafka topic `transactions` and that topic is the system's record of truth. Two consumers read it: Spark in batch (nightly, by offset range) and Flink in streaming (continuously). Flink writes scored output back to two more topics (`transactions-scored`, `fraud-alerts`).

[Kafdrop](https://github.com/obsidiandynamics/kafdrop) is a lightweight web UI mounted on top of the same broker so you can poke at topics, partitions, and consumer groups without dropping into a shell.

## Topology

| **Service** | **Image** | **Hostname** | **Host -> container ports** | **What other services talk to it** |
|-|-|-|-|-|
| `kafka` | `apache/kafka:4.3.0` | `kafka` | `9092->9092` | |
| `kafdrop` (optional) | `obsidiandynamics/kafdrop:latest` | `kafdrop` | `9001->9000` | Browser -> Kafdrop UI; Kafdrop -> `kafka:9094` |

Single-broker KRaft cluster - same node is both broker and controller. No external ZooKeeper for Kafka itself.

### The three listeners

| **Listener** | **Port** | **Used by** |
|-|-|-|
| `EXTERNAL` | 9092 | Host-machine clients - advertised as `localhost:9092` |
| `INTERNAL` | 9094 | Other docker containers - advertised as `kafka:9094` |
| `CONTROLLER` | 9093 | KRaft controller quorum (intra-cluster only) |

`KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL` says replicas intra-cluster talk over the `INTERNAL` listener. With one broker that's effectively a no-op, but if you ever scale out, this is the right default.

## Configuration

The official `apache/kafka` image uses the form `KAFKA_<KEY>` for environment variables:

| **Env var** | **Value** | **Why** |
|-|-|-|
| `KAFKA_NODE_ID` | `0` | Single node for both broker and controller |
| `KAFKA_PROCESS_ROLES` | `broker,controller` | KRaft mode |
| `KAFKA_LISTENERS` | `INTERNAL://:9094,CONTROLLER://:9093,EXTERNAL://:9092` | Bind addresses |
| `KAFKA_ADVERTISED_LISTENERS` | `INTERNAL://kafka:9094,EXTERNAL://localhost:9092` | What the broker tells clients to connect to |
| `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` | `CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT` | No TLS |
| `KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS` | `kafka:9093` | Single-voter quorum, Kraft only |
| `KAFKA_CONTROLLER_LISTENER_NAMES` | `CONTROLLER` | Listeners used by the controller, Kraft only |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | `0@kafka:9093` | Single-voter quorum, Kraft only |
| `KAFKA_INTER_BROKER_LISTENER_NAME` | `INTERNAL` | Listener name used for communication between brokers |
| `KAFKA_LOG_DIRS` | `/var/lib/kafka/data` | Pointed at the named volume |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | `true` | Producers can create topics (`transactions`/`transactions-scored`/`fraud-alerts`) on first publish |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | `1` | Required for single broker - default is 3 |
| `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACOTR` | `1` | Same as above for the transactions/exactly-once topic |
| `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR` | `1` | Min no. replicas that must acknowledge a write in order to be considered successful |
| `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS` | `0` | Tests / smoke runs don't pay the 3-second startup tax |

Kafdrop is even simpler - one var: `KAFKA_BROKERCONNECT=kafka:9094`.

## Volumes

| **Volume** | **Mount path** | **What's persisted** |
|-|-|-|
| `kafka-data` | `/var/lib/kafka/data` | Topics, partitions, KRaft metadata, offsets |

The first boot auto-formats the log directory and generates a cluster ID; subsequent boots reuse it. Kafdrop has no volume - it's purely a UI on top of the broker.

## Healthcheck

```yaml
healthcheck:
  test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:9094 > /dev/null"]
  interval: 10s
  timeout: 5s
  retries: 10
```

`kafka-broker-api-versions.sh` is the cheapest probe that actually exercises a live connection. Other services that depend on Kafka (`kafdrop`, `flink-jobmanager`) wait for `service_healthy` before starting. Kafdrop has no healthcheck.

## Why this shapes?

- **KRaft, not ZooKeeper**. KRaft is marked as production-ready as of Kafka 3.3 and the future is ZK-less. Eliminates one container and one healthcheck race.
- **Three listeners on purpose**. Cleanly separating host clients from in-network clients from the controller quorum keeps the "why isn't my consumer connecting?" question always answerable by looking at which network the client lives on.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`. Cuts the producer bring-up to one step (just publish - the broker creates the topic). For production you'd disable this and pre-create with explicit partition counts and retention. This project's three topics use defaults today; revisit when we tune transactions retention to support a long Spark batch replay.
- **`user: "0:0"` on Kafka**. Lets the entrypoint chown `/var/lib/kafka/data` on first boot when Docker creates the volume with root ownership.

### Caveats

- **Replication is hard-coded to 1**. The internal-topic RF overrides are mandatory for a single broker. If you ever add a second broker, also bump those to 2 (and add `--replication-factor 2` on every topic create), otherwise replicas won't be placed on the new broker.
- **`auto-create` has invisible defaults**. Auto-created topics get `num.partitions=1` and `default.replication.factor=1`. The `transactions` topic gets used heavily by both Spark batch and Flink streaming; if you want partition-level parallelism (Flink scoring with parallelism > 1, partitioning by `card_id`), pre-create the topic with the right partition count before the producer first publishes.
- **`make nuke` deletes all topics + offsets**.

## Alternatives

| **System** | **What is it** | **Pick instead when...** |
|-|-|-|
| **RabbitMQ** | AMQP message broker | Traditional queues + per-message ack semantics. RabbitMQ deletes messages after consumption - there's no replay-from-offset, which is the feature we need here |
| **AWS Kinesis** / **Google Pub/Sub** | Managed Kafka equivalent | You're in AWS/GCP and want zero ops. Vendor lock-in |
| **Apache Pulsar** | Kafka competitor | You need multi-tenancy + tiered storage built in. More moving parts (BookKeeper + ZooKeeper) |

Kafka won the durable replayable log category and that's exactly what we need here - Spark batch-reads the same topic Flink streams from. RabbitMQ would have us writing transactions to two places to support both consumers. Pulsar is the strongest 2026 challenger but has more operational surface

## Common commands

Use the in-network listener kafka:9094 from inside containers:

```bash
# list Kafka topics
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9094 --list

# create a topic with configs
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9094 \
    --create --topic transactions --partitions 4 --replication-factor 1

# read messages from a specific topic
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9094 --topic transactions \
    --from-beginning --max-messages 5
```

Kafka UIs:
- Kafdrop: http://localhost:9001

Smoke test:

```bash
make smoke-kafka   # ephemeral topic + produce + consume + delete
```

## References

- [If you're learning Kafka, this article is for you](https://vutr.substack.com/p/if-youre-learning-kafka-this-article)