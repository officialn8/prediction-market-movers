#!/bin/bash
set -e

# Default to collector if not specified
SERVICE_NAME="${RAILWAY_SERVICE_NAME:-collector}"

if [ "$SERVICE_NAME" = "dashboard" ]; then
    echo "Starting Dashboard..."
    exec streamlit run apps/dashboard/app.py --server.port=${PORT:-8501} --server.address=0.0.0.0
else
    echo "Starting Collector (Service: $SERVICE_NAME)..."
    exec python -m apps.collector.main
fi
