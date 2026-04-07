#!/bin/bash

# 1. Start the Cloudflare Access bridge in the background
# This maps database.knotwealth.com to localhost:${DB_PORT:-3306} inside the container
cloudflared tunnel access tcp --hostname database.knotwealth.com --listener localhost:${DB_PORT:-3306} &

# 2. Wait a moment for the bridge to initialize
sleep 5

# 3. Start the Python Application
exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080} --workers 1