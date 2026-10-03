#!/bin/bash

# Entry points shipped in /opt by docker/Dockerfile.
VALID_SERVICES=(
  proxy
  cli
  hercules
  rome-via-api
  rome-via-sync
  rome-via-enrich
  rome-audit
  apply_migrations
  cli.sh
  cli-deploy.sh
)

if [ -z "${SERVICE_NAME}" ]; then
  echo "SERVICE_NAME is not specified"
  exit 1
fi;

for service in "${VALID_SERVICES[@]}"; do
  if [ "${SERVICE_NAME}" == "${service}" ]; then
    exec "./${SERVICE_NAME}"
  fi;
done

echo "Unknown SERVICE_NAME '${SERVICE_NAME}'. Expected one of: ${VALID_SERVICES[*]}" >&2
exit 1
