#!/bin/sh

if [ "$1" = "cov" ];then

    poetry run pytest --cov=pyxxl --cov-report=html --cov-config=.coveragerc pyxxl/tests
else
    poetry run pytest --capture=no pyxxl/tests
fi
