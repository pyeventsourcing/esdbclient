.EXPORT_ALL_VARIABLES:

# SHELL = bash

# For testing with production EventStoreDB builds...
EVENTSTORE_IMAGE_NAME ?= ghcr.io/eventstore/eventstore
EVENTSTORE_IMAGE_TAG ?= 22.10.2-bullseye-slim

# For testing with Jaao's dev builds...
# EVENTSTORE_IMAGE_NAME ?= ghcr.io/thefringeninja/eventstore
# EVENTSTORE_IMAGE_TAG ?= 21.10.11-dev

POETRY ?= poetry
POETRY_INSTALLER_URL ?= https://install.python-poetry.org
PYTHONUNBUFFERED: 1

.PHONY: install-poetry
install-poetry:
	curl -sSL $(POETRY_INSTALLER_URL) | python3
	$(POETRY) --version

.PHONY: install-packages
install-packages:
	$(POETRY) --version
	$(POETRY) install --no-root -vv $(opts)

.PHONY: install
install:
	$(POETRY) --version
	$(POETRY) install -vv $(opts)

.PHONY: install-pre-commit-hooks
install-pre-commit-hooks:
ifeq ($(opts),)
	$(POETRY) run pre-commit install
endif

.PHONY: uninstall-pre-commit-hooks
uninstall-pre-commit-hooks:
ifeq ($(opts),)
	$(POETRY) run pre-commit uninstall
endif

.PHONY: lock-packages
lock-packages:
	$(POETRY) lock -vv --no-update

.PHONY: update-packages
update-packages:
	$(POETRY) update -vv

.PHONY: lint-black
lint-black:
	$(POETRY) run black --check --diff .

.PHONY: lint-flake8
lint-flake8:
	$(POETRY) run flake8

.PHONY: lint-isort
lint-isort:
	$(POETRY) run isort --check-only --diff .

.PHONY: lint-mypy
lint-mypy:
	$(POETRY) run mypy

.PHONY: lint-python
lint-python: lint-black lint-flake8 lint-isort lint-mypy

.PHONY: lint
lint: lint-python

.PHONY: fmt-black
fmt-black:
	$(POETRY) run black .

.PHONY: fmt-isort
fmt-isort:
	$(POETRY) run isort .

.PHONY: fmt
fmt: fmt-black fmt-isort

.PHONY: test
test:
	timeout --preserve-status --kill-after=10s 5m $(POETRY) run coverage run -m unittest discover ./tests -v
	$(POETRY) run coverage report --fail-under=100 --show-missing

# 	$(POETRY) run python -m pytest -v $(opts) $(call tests,.) & read -t 1 ||

# 	$(POETRY) run python -m pytest -v tests/test_docs.py
# 	$(POETRY) run python -m unittest discover tests -v

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
	  protos/esdbclient/protos/Grpc/code.proto     \
	  protos/esdbclient/protos/Grpc/shared.proto   \
	  protos/esdbclient/protos/Grpc/status.proto   \
	  protos/esdbclient/protos/Grpc/streams.proto  \
	  protos/esdbclient/protos/Grpc/persistent.proto \
	  protos/esdbclient/protos/Grpc/gossip.proto \
	  protos/esdbclient/protos/Grpc/cluster.proto

.PHONY: start-eventstoredb-insecure
start-eventstoredb-insecure:
	docker run -d -i -t -p 2113:2113 \
    --env "EVENTSTORE_ADVERTISE_HOST_TO_CLIENT_AS=localhost" \
    --env "EVENTSTORE_ADVERTISE_HTTP_PORT_TO_CLIENT_AS=2113" \
    --name my-eventstoredb-insecure \
    $(EVENTSTORE_IMAGE_NAME):$(EVENTSTORE_IMAGE_TAG) \
    --insecure

.PHONY: start-eventstoredb-secure
start-eventstoredb-secure:
	docker run -d -i -t -p 2114:2113 \
    --env "HOME=/tmp" \
    --env "EVENTSTORE_ADVERTISE_HOST_TO_CLIENT_AS=localhost" \
    --env "EVENTSTORE_ADVERTISE_HTTP_PORT_TO_CLIENT_AS=2114" \
    --name my-eventstoredb-secure \
    $(EVENTSTORE_IMAGE_NAME):$(EVENTSTORE_IMAGE_TAG) \
    --dev

.PHONY: attach-eventstoredb-insecure
attach-eventstoredb-insecure:
	docker exec -it my-eventstoredb-insecure /bin/bash

.PHONY: attach-eventstoredb-secure
attach-eventstoredb-secure:
	docker exec -it my-eventstoredb-secure /bin/bash

.PHONY: stop-eventstoredb-insecure
stop-eventstoredb-insecure:
	docker stop my-eventstoredb-insecure
	docker rm my-eventstoredb-insecure

.PHONY: stop-eventstoredb-secure
stop-eventstoredb-secure:
	docker stop my-eventstoredb-secure
	docker rm my-eventstoredb-secure

.PHONY: start-eventstoredb
start-eventstoredb: start-eventstoredb-insecure start-eventstoredb-secure docker-up

.PHONY: stop-eventstoredb
stop-eventstoredb: stop-eventstoredb-insecure stop-eventstoredb-secure docker-down

.PHONY: docker-pull
docker-pull:
	docker compose pull

.PHONY: docker-build
docker-build:
	docker compose build

.PHONY: docker-up
docker-up:
	@docker --version
	docker compose up -d
	@echo "Waiting for containers to be healthy"
	@until docker compose ps | grep -in "healthy" | wc -l | grep -in 3 > /dev/null; do printf "." && sleep 1; done; echo ""
	@docker compose ps
	@sleep 15

.PHONY: docker-stop
docker-stop:
	docker compose stop

.PHONY: docker-down
docker-down:
	docker compose down -v --remove-orphans


.PHONY: docker-logs
docker-logs:
	docker compose logs --follow --tail=1000

