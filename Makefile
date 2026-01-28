.PHONY: up-prod up-dev up-local-debug down logs

# Run in production mode (AWS S3)
up-prod:
	docker-compose --env-file .env.prod up -d

# Run in development mode (MinIO)
up-dev:
	docker-compose --env-file .env.dev up -d

# Run in local debug mode (filesystem)
up-local-debug:
	docker-compose --env-file .env.local up -d

# Stop all services
down:
	docker-compose down

# Show logs
logs:
	docker-compose logs -f dataops-assistant

# Rebuild and run in local debug
rebuild-local:
	docker-compose --env-file .env.local up --build -d

# Rebuild and run in dev
rebuild-dev:
	docker-compose --env-file .env.dev up --build -d

# Rebuild and run in prod
rebuild-prod:
	docker-compose --env-file .env.prod up --build -d
