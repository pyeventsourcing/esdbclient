.EXPORT_ALL_VARIABLES:

POETRY ?= poetry
POETRY_INSTALLER_URL ?= https://install.python-poetry.org


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
	$(POETRY) run python -m pytest -v $(opts) $(call tests,.)
# 	$(POETRY) run python -m pytest -v tests/test_docs.py
# 	$(POETRY) run python -m unittest discover tests -v

.PHONY: build
build:
	$(POETRY) build
# 	$(POETRY) build -f sdist    # build source distribution only

.PHONY: publish
publish:
	$(POETRY) publish

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
	  protos/esdbclient/protos/Grpc/persistent.proto
# 	  protos/esdbclient/protos/command.proto \
# 	  protos/esdbclient/protos/control.proto \
# 	  protos/esdbclient/protos/event.proto \
# 	  protos/esdbclient/protos/query.proto

.PHONY: start-eventstoredb-21-10-insecure
start-eventstoredb-21-10-insecure:
	docker run -d --name my-eventstoredb-insecure -it -p 2114:2113 eventstore/eventstore:21.10.9-buster-slim --insecure

.PHONY: start-eventstoredb-21-10-secure
start-eventstoredb-21-10-secure:
	docker run -d --name my-eventstoredb-secure -it -p 2113:2113 --env "HOME=/tmp" eventstore/eventstore:21.10.9-buster-slim --dev

.PHONY: start-eventstoredb-22-10-insecure
start-eventstoredb-22-10-insecure:
	docker run -d --name my-eventstoredb-insecure -it -p 2114:2113 eventstore/eventstore:22.10.0-buster-slim --insecure

.PHONY: start-eventstoredb-22-10-secure
start-eventstoredb-22-10-secure:
	docker run -d --name my-eventstoredb-secure -it -p 2113:2113 --env "HOME=/tmp" eventstore/eventstore:22.10.0-buster-slim --dev

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
