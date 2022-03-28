#!/bin/sh

if [ "$1" = "cov" ];then

    pytest --cov=pyxxl --cov-report=html --cov-config=.coveragerc pyxxl/tests
else
    pytest --capture=no pyxxl/tests
fi