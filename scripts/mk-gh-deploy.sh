#!/usr/bin/env bash
set -e
set -x

pip install -e ".[doc]"
cd ./docs && mkdocs gh-deploy
