#!/usr/bin/env bash
set -e
set -x


cd ./docs && mkdocs gh-deploy
