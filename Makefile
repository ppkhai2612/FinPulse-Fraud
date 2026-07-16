COMPOSE ?= docker compose

.PHONY: up-core down down-volume smoke smoke-hdfs smoke-spark

up-core:
	${COMPOSE} up -d namenode datanode-1 datanode-2 spark-master spark-worker-1 kafka

down:
	${COMPOSE} down

down-volume:
	${COMPOSE} down -v


smoke: smoke-hdfs smoke-spark

smoke-hdfs:
	@bash scripts/smoke.sh hdfs

smoke-spark:
	@bash scripts/smoke.sh spark

smoke-kafka:
	@bash scripts/smoke.sh kafka

smoke-pinot:
	@bash scripts/smoke.sh pinot

all:
	@echo "This make line will not be printed"
	echo "But this will"



up-hdfs:
	${COMPOSE} up -d namenode datanode-1 datanode-2

up-spark:
	${COMPOSE} up -d spark-master spark-worker-1

up-kafka:
	${COMPOSE} up -d kafka producer

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