#!/bin/bash
echo "Starting Verixia..."

# Start Qdrant if not running
if ! curl -s http://127.0.0.1:6335/collections > /dev/null 2>&1; then
    echo "  Starting Qdrant..."
    /home/lexiegirl/Desktop/gamewriter/qdrant \
        --config-path /home/lexiegirl/Desktop/verixia/qdrant_config/config.yaml \
        > /mnt/kayla_archive/verixia/journal/qdrant.log 2>&1 &
    sleep 5
fi

POINTS=$(curl -s http://127.0.0.1:6335/collections/verixia_legal | \
    python3 -m json.tool 2>/dev/null | grep points_count | grep -o '[0-9]*')
echo "  Qdrant: verixia_legal — ${POINTS} points"

cd /home/lexiegirl/Desktop/verixia
source venv/bin/activate
export VERIXIA_DEV_MODE=true
VERIXIA_DEV_MODE=true nohup python3 -m uvicorn api.main:app \
    --host 0.0.0.0 \
    --port 8790 \
    --workers 1 \
    > /mnt/kayla_archive/verixia/journal/api.log 2>&1 &

sleep 3
echo "  API: http://localhost:8790"
echo "  UI:  http://localhost:8790/ui"
echo "Verixia running."
