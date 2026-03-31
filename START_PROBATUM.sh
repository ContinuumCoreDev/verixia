#!/bin/bash
# Start Probatum — Qdrant + API

echo "Starting Probatum Qdrant on port 6335..."
/home/lexiegirl/Desktop/gamewriter/qdrant \
    --config-path /home/lexiegirl/Desktop/probatum/qdrant_config/config.yaml &
sleep 3

echo "Starting Probatum API on port 8790..."
cd /home/lexiegirl/Desktop/probatum
source venv/bin/activate
uvicorn api.main:app --host 127.0.0.1 --port 8790 --reload &

echo "Probatum running."
echo "  Local UI:  http://127.0.0.1:8790/ui"
echo "  AWS API:   http://100.28.252.218:8790"
echo "  Docs:      http://127.0.0.1:8790/docs"
