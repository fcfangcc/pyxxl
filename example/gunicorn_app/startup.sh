#!/bin/sh

gunicorn -c gunicorn.conf.py app:app -b 0.0.0.0:9000
