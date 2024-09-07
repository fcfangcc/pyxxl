#!/usr/bin/env bash
set -e
set -x
pytest --cov=pyxxl --cov-report=term-missing pyxxl/tests "${@}"
