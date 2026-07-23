COMPOSE = docker compose

# HMS Postgres JAR
HIVE_PG_JAR_VERSION = 42.7.2
HIVE_PG_JAR_PATH = docker/hive-metastore/jars/postgresql-$(HIVE_PG_JAR_VERSION).jar

# .PHONY: up-core down down-volume smoke smoke-hdfs smoke-spark

up:
	${COMPOSE} up -d

up-core:
	${COMPOSE} up -d namenode datanode-1 datanode-2 spark-master spark-worker-1 kafka kafka-producer

up-bi:
	${COMPOSE} up -d pinot-zookeeper pinot-controller pinot-broker pinot-server superset

up-airflow:
	${COMPOSE} up postgres airflow-init airflow-apiserver airflow-scheduler airflow-dag-processor

down:
	${COMPOSE} down

down-volume:
	${COMPOSE} down -v

down-airflow:
	${COMPOSE} down postgres airflow-init airflow-apiserver airflow-scheduler airflow-dag-processor

down-spark:
	${COMPOSE} down spark-master spark-worker-1

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

all:
	@echo "This make line will not be printed"
	echo "But this will"



up-hdfs:
	${COMPOSE} up -d namenode datanode-1 datanode-2

up-spark:
	${COMPOSE} up -d spark-master spark-worker-1

up-kafka:
	${COMPOSE} up -d kafka kafka-producer

up-pinot:
	${COMPOSE} up -d pinot-zookeeper pinot-controller pinot-broker pinot-server

up-dwh:
	${COMPOSE} up metastore-db hive-metastore-init hive-metastore

up-superset:
	${COMPOSE} up -d superset

down-pinot:
	${COMPOSE} down pinot-zookeeper pinot-controller pinot-broker pinot-server


hive-deps:
	@mkdir -p $(dir $(HIVE_PG_JAR_PATH))
	@if [ -f $(HIVE_PG_JAR_PATH) ]; then \
	  echo "$(HIVE_PG_JAR_PATH) already present"; \
	else
	  echo "Downloading postgresql-$(HIVE_PG_JAR_VERSION).jar -> $(HIVE_PG_JAR_PATH)"; \
	  curl -fsSL "https://jdbc.postgresql.org/download/postgresql-$(HIVE_PG_JAR_VERSION).jar" \
	    -o $(HIVE_PG_JAR_PATH); \
	fi