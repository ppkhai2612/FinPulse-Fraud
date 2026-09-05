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



## Volumes

| **Volume** | **Mount path** | **What's persisted** |
|-|-|-|
| `kafka-data` | `/var/lib/kafka/data` | Topics, partitions, KRaft metadata, offsets |

The first boot auto-formats the log directory and generates a cluster ID; subsequent boots reuse it. Kafdrop has no volume - it's purely a UI on top of the broker.

## Healthcheck

## Why this shapes?

### Caveats

## Alternatives

## Common commands


## References

[If you're learning Kafka, this article is for you](https://vutr.substack.com/p/if-youre-learning-kafka-this-article)
