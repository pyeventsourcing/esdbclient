# Python gRPC Client for EventStoreDB

## Installation

Use pip to install this package from the
[the Python Package Index](https://pypi.org/project/esdbclient/).

    $ pip install esdbclient

It is recommended to install Python packages into a Python virtual environment.

## Getting started

...

## Developers

Clone the project repository, set up a virtual environment, and install
dependencies.

Use your IDE (e.g. PyCharm) to open the project repository. Create a
Poetry virtual environment, and then update packages.

    $ make update-packages

Alternatively, use the ``make install`` command to create a dedicated
Python virtual environment for this project.

    $ make install

The ``make install`` command uses the build tool Poetry to create a dedicated
Python virtual environment for this project, and installs popular development
dependencies such as Black, isort and pytest.

Add tests in `./tests`. Add code in `./esdbclient`.

Run tests.

    $ make test

Check the formatting of the code.

    $ make lint

Reformat the code.

    $ make fmt

Add dependencies in `pyproject.toml` and then update installed packages.

    $ make update-packages
