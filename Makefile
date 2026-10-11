.PHONY: install server worker-orders worker-ai worker api demo schedule test up down clean dev

export PYTHONPATH := src

install:
	pip install -e ".[dev,openai,anthropic]"
	cp -n .env.example .env || true

# Temporal dev server with the custom search attributes this project uses.
# Install the CLI first: brew install temporal  (or see docs/04-running-it.md)
server:
	temporal server start-dev \
	  --db-filename ./data/temporal.db \
	  --search-attribute OrderStatus=Keyword \
	  --search-attribute CustomerId=Keyword

worker-orders:
	python -m shopsphere.worker orders

worker-ai:
	python -m shopsphere.worker ai

worker:
	python -m shopsphere.worker all

api:
	uvicorn shopsphere.api.main:app --reload --port 8000

demo:
	python scripts/demo.py

schedule:
	python scripts/create_schedule.py --every 2m

test:
	pytest -v

up:
	docker compose up --build

down:
	docker compose down

clean:
	rm -rf data/*.db .pytest_cache

dev:
	@echo "Open four terminals:"
	@echo "  1) make server"
	@echo "  2) make worker-orders"
	@echo "  3) make worker-ai"
	@echo "  4) make api"
	@echo "then: make demo   and open http://localhost:8233"
