FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir ".[openai,anthropic]"

COPY scripts ./scripts
RUN mkdir -p /app/data

CMD ["python", "-m", "shopsphere.worker", "all"]
