COMPOSE = docker compose

# HMS Postgres JAR
HIVE_PG_JAR_VERSION = 42.7.2
HIVE_PG_JAR_PATH = docker/hive-metastore/jars/postgresql-$(HIVE_PG_JAR_VERSION).jar

.PHONY: up-core down down-volume smoke smoke-hdfs smoke-spark

up-core:
	${COMPOSE} up -d namenode datanode-1 datanode-2 spark-master spark-worker-1 kafka

up-bi:
	${COMPOSE} up -d pinot-zookeeper pinot-controller pinot-broker pinot-server
		superset

down:
	${COMPOSE} down

down-volume:
	${COMPOSE} down -v


smoke: smoke-hdfs smoke-spark smoke-kafka smoke-pinot smoke-trino

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



curate-jobs:
	${COMPOSE} exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    path_to_python_file`

	${COMPOSE} exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 /opt/spark/work-dir/jobs/enrich/build_enriched_fact.py
	docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/jobs/enrich/check_fraud_rate.py`
	docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/jobs/features/build_customer_features.py


hive-deps:
	@mkdir -p $(dir $(HIVE_PG_JAR_PATH))
	@if [ -f $(HIVE_PG_JAR_PATH) ]; then \
	  echo "$(HIVE_PG_JAR_PATH) already present"; \
	else
	  echo "Downloading postgresql-$(HIVE_PG_JAR_VERSION).jar -> $(HIVE_PG_JAR_PATH)"; \
	  curl -fsSL "https://jdbc.postgresql.org/download/postgresql-$(HIVE_PG_JAR_VERSION).jar" \
	    -o $(HIVE_PG_JAR_PATH); \
	fi