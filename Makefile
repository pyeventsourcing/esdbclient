.EXPORT_ALL_VARIABLES:

# SHELL = bash

# KURRENTDB_DOCKER_IMAGE ?= eventstore/eventstore:21.10.9-buster-slim
# KURRENTDB_DOCKER_IMAGE ?= docker.eventstore.com/eventstore-ce/eventstoredb-ce:22.10.4-jammy
# KURRENTDB_DOCKER_IMAGE ?= docker.eventstore.com/eventstore-ce/eventstoredb-ce:23.10.0-jammy
# KURRENTDB_DOCKER_IMAGE ?= docker.eventstore.com/eventstore-ce/eventstoredb-oss:24.6.0-jammy
# KURRENTDB_DOCKER_IMAGE ?= docker.eventstore.com/eventstore/eventstoredb-ee:24.10.0-x64-8.0-bookworm-slim
# KURRENTDB_DOCKER_IMAGE ?= docker.eventstore.com/eventstore-ce/eventstoredb-ce:24.2.0-alpha.115-jammy
# KURRENTDB_DOCKER_IMAGE ?= docker.eventstore.com/eventstore-staging-ce/eventstoredb-ce:24.6.0-nightly-x64-8.0-jammy

# list tags for 23.10: https://docker.cloudsmith.io/v2/eventstore/eventstore-ce/eventstoredb-oss/tags/list
# KURRENTDB_DOCKER_IMAGE ?= docker.cloudsmith.io/eventstore/eventstore-ce/eventstoredb-oss:23.10.7-bookworm-slim

# list tags for 24.10: https://docker.kurrent.io/v2/eventstore/eventstoredb-ee/tags/list

#KURRENTDB_DOCKER_IMAGE ?= kurrentplatform/kurrentdb:25.1.0-experimental-arm64-8.0-jammy

# What's in the build matrix:

#KURRENTDB_DOCKER_IMAGE ?= docker.cloudsmith.io/eventstore/eventstore-ce/eventstoredb-oss:23.10.7-bookworm-slim
#KURRENTDB_DOCKER_IMAGE ?= docker.cloudsmith.io/eventstore/eventstore/eventstoredb-ee:24.10.6-x64-8.0-bookworm-slim
#KURRENTDB_DOCKER_IMAGE ?= docker.kurrent.io/kurrent-latest/kurrentdb:25.0.1-x64-8.0-bookworm-slim
KURRENTDB_DOCKER_IMAGE ?= docker.kurrent.io/kurrent-latest/kurrentdb:25.1.0-x64-8.0-bookworm-slim

NOCOVER_TAGS ?= v2

PYTHONUNBUFFERED=1
PYTHONPATH=./tests
SAMPLES_LINE_LENGTH=70

POETRY_VERSION=2.2.1
POETRY ?= poetry@$(POETRY_VERSION)

.PHONY: install-poetry
install-poetry:
	@pipx install --suffix="@$(POETRY_VERSION)" "poetry==$(POETRY_VERSION)"
	$(POETRY) --version

.PHONY: install
install:
	$(POETRY) sync --all-extras $(opts)

.PHONY: update
update: update-lock install

.PHONY: update-lock
update-lock:
	$(POETRY) update --lock -v

.PHONY: fmt
fmt: fmt-isort fmt-black fmt-ruff

.PHONY: fmt-black
fmt-black:
	$(POETRY) run black --extend-exclude=samples .
	$(POETRY) run black --line-length=$(SAMPLES_LINE_LENGTH) ./samples

.PHONY: fmt-ruff
fmt-ruff:
	$(POETRY) run ruff --fix kurrentdbclient tests

.PHONY: fmt-ruff-unsafe
fmt-ruff-unsafe:
	$(POETRY) run ruff --fix --unsafe-fixes kurrentdbclient tests


.PHONY: fmt-isort
fmt-isort:
	$(POETRY) run isort --extend-skip=samples .
	$(POETRY) run isort --line-length=$(SAMPLES_LINE_LENGTH) samples

.PHONY: lint
lint: lint-python

.PHONY: lint-black
lint-black:
	$(POETRY) run black --check --diff --extend-exclude samples .
	$(POETRY) run black --check --diff --line-length=$(SAMPLES_LINE_LENGTH) ./samples

.PHONY: lint-ruff
lint-ruff:
	$(POETRY) run ruff check .

#.PHONY: lint-flake8
#lint-flake8:
#	$(POETRY) run flake8

.PHONY: lint-isort
lint-isort:
	$(POETRY) run isort --check-only --diff --extend-skip-glob samples .
	$(POETRY) run isort --check-only --diff --line-length=$(SAMPLES_LINE_LENGTH) samples

.PHONY: lint-mypy
lint-mypy:
	$(POETRY) run mypy --strict

.PHONY: lint-python
lint-python: lint-black lint-ruff lint-isort lint-mypy

.PHONY: test
test:
	@timeout --preserve-status --kill-after=10s 10m $(POETRY) run python ./runtests.py
#	$(POETRY) run coverage report

# 	$(POETRY) run python -m pytest -v $(opts) $(call tests,.) & read -t 1 ||

# 	$(POETRY) run python -m pytest -v tests/test_docs.py
# 	$(POETRY) run python -m unittest discover tests -v

.PHONY: benchmark
benchmark:
	$(POETRY) run python tests/benchmark.py

.PHONY: build
build:
	$(POETRY) build
# 	$(POETRY) build -f sdist    # build source distribution only

.PHONY: publish
publish:
	$(POETRY) publish

# Orig proto files: https://github.com/EventStore/EventStore/tree/master/src/Protos/Grpc
.PHONY: grpc-stubs
grpc-stubs:
	$(POETRY) run python -m grpc_tools.protoc \
	  --proto_path=./protos \
	  --python_out=. \
	  --grpc_python_out=. \
	  --mypy_out=. \
	  protos/kurrentdbclient/protos/google/rpc/code.proto     \
	  protos/kurrentdbclient/protos/kurrent/rpc/errors.proto     \
	  protos/kurrentdbclient/protos/kurrent/rpc/rpc.proto     \
	  protos/kurrentdbclient/protos/v1/shared.proto   \
	  protos/kurrentdbclient/protos/v1/status.proto   \
	  protos/kurrentdbclient/protos/v1/streams.proto  \
	  protos/kurrentdbclient/protos/v1/persistent.proto \
	  protos/kurrentdbclient/protos/v1/gossip.proto \
	  protos/kurrentdbclient/protos/v1/cluster.proto \
	  protos/kurrentdbclient/protos/v1/projections.proto \
	  protos/kurrentdbclient/protos/v2/streams/errors.proto \
	  protos/kurrentdbclient/protos/v2/streams/streams.proto

.PHONY: start-kurrentdb-insecure
start-kurrentdb-insecure:
	@docker run -d -i -t -p 2113:2113 \
    --env "KURRENTDB_ADVERTISE_HOST_TO_CLIENT_AS=localhost" \
    --env "KURRENTDB_ADVERTISE_NODE_PORT_TO_CLIENT_AS=2113" \
    --env "KURRENTDB_RUN_PROJECTIONS=All" \
    --env "KURRENTDB_START_STANDARD_PROJECTIONS=true" \
    --env "KURRENTDB_ENABLE_ATOM_PUB_OVER_HTTP=true" \
    --env "KURRENTDB_ALLOW_UNKNOWN_OPTIONS=true" \
    --env "EVENTSTORE_ADVERTISE_HOST_TO_CLIENT_AS=localhost" \
    --env "EVENTSTORE_ADVERTISE_NODE_PORT_TO_CLIENT_AS=2113" \
    --env "EVENTSTORE_RUN_PROJECTIONS=All" \
    --env "EVENTSTORE_START_STANDARD_PROJECTIONS=true" \
    --env "EVENTSTORE_ENABLE_ATOM_PUB_OVER_HTTP=true" \
    --name my-kurrentdb-insecure \
    $(KURRENTDB_DOCKER_IMAGE) \
    --insecure

.PHONY: start-kurrentdb-secure
start-kurrentdb-secure:
	@docker run -d -i -t -p 2114:2113 \
    --env "HOME=/tmp" \
    --env "KURRENTDB_ADVERTISE_HOST_TO_CLIENT_AS=localhost" \
    --env "KURRENTDB_ADVERTISE_NODE_PORT_TO_CLIENT_AS=2114" \
    --env "KURRENTDB_RUN_PROJECTIONS=All" \
    --env "KURRENTDB_START_STANDARD_PROJECTIONS=true" \
    --env "KURRENTDB_ALLOW_UNKNOWN_OPTIONS=true" \
    --env "EVENTSTORE_ADVERTISE_HOST_TO_CLIENT_AS=localhost" \
    --env "EVENTSTORE_ADVERTISE_NODE_PORT_TO_CLIENT_AS=2114" \
    --env "EVENTSTORE_RUN_PROJECTIONS=All" \
    --env "EVENTSTORE_START_STANDARD_PROJECTIONS=true" \
    --name my-kurrentdb-secure \
    $(KURRENTDB_DOCKER_IMAGE) \
    --dev

.PHONY: start-kurrentdb-secure-v21-10-9
start-kurrentdb-secure-21-10-9:
	@docker run -d -i -t -p 2114:2113 \
    --env "HOME=/tmp" \
    --env "KURRENTDB_ADVERTISE_HOST_TO_CLIENT_AS=localhost" \
    --env "KURRENTDB_ADVERTISE_HTTP_PORT_TO_CLIENT_AS=2114" \
    --env "KURRENTDB_RUN_PROJECTIONS=All" \
    --env "KURRENTDB_START_STANDARD_PROJECTIONS=true" \
    --name my-kurrentdb-secure \
    eventstore/eventstore:21.10.9-buster-slim \
    --dev

.PHONY: attach-kurrentdb-insecure
attach-kurrentdb-insecure:
	@docker exec -it my-kurrentdb-insecure /bin/bash

.PHONY: attach-kurrentdb-secure
attach-kurrentdb-secure:
	@docker exec -it my-kurrentdb-secure /bin/bash

.PHONY: stop-kurrentdb-insecure
stop-kurrentdb-insecure:
	@docker stop my-kurrentdb-insecure
	@docker rm my-kurrentdb-insecure

.PHONY: stop-kurrentdb-secure
stop-kurrentdb-secure:
	@docker stop my-kurrentdb-secure
	@docker rm my-kurrentdb-secure

.PHONY: start-kurrentdb
start-kurrentdb: start-kurrentdb-insecure start-kurrentdb-secure docker-up

.PHONY: stop-kurrentdb
stop-kurrentdb: stop-kurrentdb-insecure stop-kurrentdb-secure docker-down

.PHONY: docker-pull
docker-pull:
	@docker compose pull

.PHONY: docker-build
docker-build:
	@docker compose build

.PHONY: docker-up
docker-up:
	@docker --version
	@docker compose up -d
	@echo "Waiting for containers to be healthy"
	@until docker compose ps | grep -in "healthy" | wc -l | grep -in 3 > /dev/null; do printf "." && sleep 1; done; echo ""
	@docker compose ps
	@sleep 15

.PHONY: docker-stop
docker-stop:
	@docker compose stop

.PHONY: docker-down
docker-down:
	@docker compose down -v --remove-orphans


.PHONY: docker-logs
docker-logs:
	@docker compose logs --follow --tail=1000

.PHONY: docker-compose-ps
docker-compose-ps:
	@docker compose ps


# Jaeger natively supports OTLP to receive trace data. You can run Jaeger in a docker container
# with the UI accessible on port 16686 and OTLP enabled on ports 4317 and 4318.
# https://opentelemetry.io/docs/languages/python/exporters/#jaeger
.PHONY: start-jaeger
start-jaeger:
	@docker run -d \
    -e COLLECTOR_ZIPKIN_HOST_PORT=:9411 \
    -p 16686:16686 \
    -p 4317:4317 \
    -p 4318:4318 \
    -p 9411:9411 \
    --name jaeger \
    jaegertracing/all-in-one:latest

.PHONY: stop-jaeger
stop-jaeger:
	@docker stop jaeger
	@docker rm jaeger
