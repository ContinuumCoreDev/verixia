#!/bin/bash
# Deploy to EC2 — excludes local-only config
rsync -avz \
    --exclude 'venv' \
    --exclude '__pycache__' \
    --exclude '.git' \
    --exclude 'data/' \
    --exclude 'qdrant_storage/' \
    --exclude 'config/config.yaml' \
    -e "ssh -i ~/.ssh/verixia-key.pem" \
    /home/lexiegirl/Desktop/verixia/ \
    ubuntu@13.219.24.216:/home/ubuntu/verixia/

ssh -i ~/.ssh/verixia-key.pem ubuntu@13.219.24.216 \
    "sudo systemctl restart verixia && sleep 4 && systemctl status verixia --no-pager | tail -3"
