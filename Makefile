COMPOSE ?= docker compose

.PHONY: up-core down down-volume smoke smoke-hdfs smoke-spark

up-core:
	${COMPOSE} up -d namenode datanode-1 datanode-2 spark-master spark-worker-1 spark-worker-2 kafka

down:
	${COMPOSE} down

down-volume:
	${COMPOSE} down -v


smoke: smoke-hdfs smoke-spark

smoke-hdfs:
	@bash scripts/smoke.sh hdfs

smoke-spark:
	@bash scripts/smoke.sh spark

all:
	@echo "This make line will not be printed"
	echo "But this will"