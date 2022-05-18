#!/usr/bin/env bash
set -e
set -x
poetry run pytest --capture=no --cov=pyxxl --cov-report=term-missing pyxxl/tests "${@}"
