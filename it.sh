#!/bin/bash
function finish {
  # stop docker services
  docker-compose -f src/it/resources/docker-compose.yml down
}
trap finish EXIT
# start docker services
docker-compose -f src/it/resources/docker-compose.yml up --build -d && \
# run the test
docker exec \
-it \
-w /app \
sbt \
sbt "+it:test"
