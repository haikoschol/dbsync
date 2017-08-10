#!/usr/bin/env sh

docker run --rm -a STDOUT -a STDERR --name dbsync-testdb -p 5434:5432 -e POSTGRES_PASSWORD=postgres mdillon/postgis:9.6-alpine

