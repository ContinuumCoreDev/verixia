#!/bin/bash
echo "Stopping Verixia..."
pkill -f "uvicorn api.main" 2>/dev/null && echo "  API stopped." || echo "  API was not running."
pkill -f "qdrant --config-path.*verixia" 2>/dev/null && echo "  Qdrant stopped." || echo "  Qdrant was not running."
echo "Done."
