FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY packages/ ./packages/
COPY migrations/ ./migrations/
COPY apps/collector/ ./apps/collector/

# Set Python path to include packages
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Run the collector
CMD ["python", "-m", "apps.collector.main"]

