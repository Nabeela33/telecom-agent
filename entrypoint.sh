#!/bin/bash
# entrypoint.sh
# Force Streamlit to use Cloud Run's expected port
PORT=${PORT:-8080}
echo "Starting Streamlit on port ${PORT}..."
exec streamlit run app.py --server.port=${PORT} --server.address=0.0.0.0 --server.headless=true
