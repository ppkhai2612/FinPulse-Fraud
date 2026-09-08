COMPOSE = docker compose

# Postgres JDBC driver for Hive Metastore (downloaded by `make hive-deps`).
HIVE_PG_JAR_VERSION = 42.7.2
HIVE_PG_JAR_PATH = docker/hive-metastore/jars/postgresql-$(HIVE_PG_JAR_VERSION).jar

# Flink SQL Kafka connector (downloaded by `make flink-deps`). The base
# flink:1.19 image does NOT bundle a Kafka connector - only the files
# connector - so any Kafka source/sink job needs this jar on the classpath
FLINK_KAFKA_JAR_VERSION = 3.2.0-1.19
FLINK_KAFKA_JAR_PATH = docker/flink/lib/flink-sql-connector-kafka-$(FLINK_KAFKA_JAR_VERSION).jar

# .PHONY: up-core up-kafka down down-volume smoke smoke-hdfs smoke-spark



hive-deps:
	@mkdir -p $(dir $(HIVE_PG_JAR_PATH))
	@if [ -f $(HIVE_PG_JAR_PATH) ]; then \
	  echo "$(HIVE_PG_JAR_PATH) already present"; \
	else
	  echo "Downloading postgresql-$(HIVE_PG_JAR_VERSION).jar -> $(HIVE_PG_JAR_PATH)"; \
	  curl -fsSL "https://jdbc.postgresql.org/download/postgresql-$(HIVE_PG_JAR_VERSION).jar" \
	    -o $(HIVE_PG_JAR_PATH); \
	fi

flink-deps:
	@mkdir -p $(dir $(FLINK_KAFKA_JAR_PATH))
	@if [ -f $(FLINK_KAFKA_JAR_PATH) ]; then \
	  echo "$(FLINK_KAFKA_JAR_PATH) already present"; \
	else \
	  echo "Downloading flink-sql-connector-kafka-$(FLINK_KAFKA_JAR_VERSION).jar -> $(FLINK_KAFKA_JAR_PATH)"; \
	  curl -fsSL "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/$(FLINK_KAFKA_JAR_VERSION)/flink-sql-connector-kafka-$(FLINK_KAFKA_JAR_VERSION).jar" \
	    -o $(FLINK_KAFKA_JAR_PATH); \
	fi

up:
	${COMPOSE} up -d

up-core: up-hdfs up-kafka up-spark

up-stream: flink-deps
	$(COMPOSE) up -d kafka kafdrop flink-jobmanager flink-taskmanager

up-bi: up-pinot up-superset up-hms up-trino

down:
	${COMPOSE} down

nuke:
	${COMPOSE} down -v

# --- SMOKE ---
smoke: smoke-hdfs smoke-spark smoke-kafka smoke-airflow smoke-pinot smoke-trino

smoke-hdfs:
	@bash scripts/smoke.sh hdfs

smoke-spark:
	@bash scripts/smoke.sh spark

smoke-kafka:
	@bash scripts/smoke.sh kafka

smoke-airflow:
	@bash scripts/smoke.sh airflow

smoke-pinot:
	@bash scripts/smoke.sh pinot

smoke-flink:
	@bash scripts/smoke.sh flink

smoke-trino:
	@bash scripts/smoke.sh trino

# --- STACKS ---
up-hdfs:
	${COMPOSE} up -d namenode datanode-1 datanode-2

up-spark:
	${COMPOSE} up -d spark-master spark-worker-1 spark-worker-2

up-kafka:
	${COMPOSE} up -d kafka kafka-producer

up-airflow:
	${COMPOSE} up -d postgres airflow-init airflow-apiserver airflow-scheduler airflow-dag-processor

up-pinot:
	${COMPOSE} up -d pinot-zookeeper pinot-controller pinot-broker pinot-server

up-superset:
	${COMPOSE} up -d superset
	
up-hms:
	${COMPOSE} up -d metastore-db hive-metastore-init hive-metastore

up-trino:
	${COMPOSE} up -d trino-coordinator


