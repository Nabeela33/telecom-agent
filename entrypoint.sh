#!/bin/bash
set -e

echo "Starting Telecom Query Agent (Gemini 2.5)..."

# Activate any environment setup (optional)
# e.g. export GOOGLE_APPLICATION_CREDENTIALS="/app/key.json"

# Start Streamlit
exec streamlit run app.py --server.port=$PORT --server.address=0.0.0.0 --server.headless=true
