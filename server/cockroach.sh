#! /usr/bin/env sh

docker run -d \
    -p 26257:26257 \
    -p 8080:8080 \
    -v $(pwd)/data/crdb:/cockroach/cockroach-data \
    cockroachdb/cockroach:v2.1.3 \
    start --insecure
