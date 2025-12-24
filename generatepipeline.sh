#!/bin/zsh
docker compose exec dataops-assistant python -m runners.generate_pipeline "$@"
