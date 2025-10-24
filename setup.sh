#!/bin/sh
# Create the shared Docker network for cross-compose communication
# Safe to run multiple times (will not error if already exists)
docker network create dataops-assistant-net || true
