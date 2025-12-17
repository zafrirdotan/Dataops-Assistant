#!/bin/zsh
docker compose exec dataops-assistant python -m app.runners.run_chat_pipeline "$@"
