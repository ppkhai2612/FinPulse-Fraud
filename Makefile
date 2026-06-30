COMPOSE ?= docker compose

.PHONY: up-core down down-volume

up-core:
	${COMPOSE} up -d namenode datanode-1 datanode-2

down:
	${COMPOSE} down

down-volume:
	${COMPOSE} down -v