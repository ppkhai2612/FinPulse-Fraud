COMPOSE = docker compose

# HMS Postgres JAR
HIVE_PG_JAR_VERSION = 42.7.2
HIVE_PG_JAR_PATH = docker/hive-metastore/jars/postgresql-$(HIVE_PG_JAR_VERSION).jar

# .PHONY: up-core up-kafka down down-volume smoke smoke-hdfs smoke-spark

# --- MAIN ---
up:
	${COMPOSE} up -d

up-core: up-hdfs up-kafka up-spark

up-stream: up-kafka up-spark up-pinot

up-bi: up-pinot up-superset up-hms up-trino

down:
	${COMPOSE} down

down-volume:
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

# Dependency for Hive
hive-deps:
	@mkdir -p $(dir $(HIVE_PG_JAR_PATH))
	@if [ -f $(HIVE_PG_JAR_PATH) ]; then \
	  echo "$(HIVE_PG_JAR_PATH) already present"; \
	else
	  echo "Downloading postgresql-$(HIVE_PG_JAR_VERSION).jar -> $(HIVE_PG_JAR_PATH)"; \
	  curl -fsSL "https://jdbc.postgresql.org/download/postgresql-$(HIVE_PG_JAR_VERSION).jar" \
	    -o $(HIVE_PG_JAR_PATH); \
	fi