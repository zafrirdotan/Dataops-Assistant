#!/bin/zsh
docker compose exec dataops-assistant python -m app.runners.generate_pipeline "$@"
