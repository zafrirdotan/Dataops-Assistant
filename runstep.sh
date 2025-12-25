#!/bin/zsh
# Usage: ./runstep.sh <step_number> [args...]
docker compose exec dataops-assistant python -m runners.step_runner "$@"
