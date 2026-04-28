"""
Verixia — FastAPI Application Entry Point
"""

import logging
import os
import warnings

# Silence model loading noise before anything else
os.environ["TRANSFORMERS_VERBOSITY"] = "error"
warnings.filterwarnings("ignore")

import transformers
transformers.logging.set_verbosity_error()

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt= "%Y-%m-%d %H:%M:%S",
)

# Silence noisy loggers
for noisy in ["httpx", "httpcore", "sentence_transformers",
              "transformers", "torch"]:
    logging.getLogger(noisy).setLevel(logging.ERROR)

logger = logging.getLogger("verixia.api")

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

limiter = Limiter(key_func=get_remote_address, default_limits=["60/minute"])

app = FastAPI(
    title       = "Verixia",
    description = "AI outputs, proven. Verification API for AI-generated claims.",
    version     = "0.1.0",
    docs_url    = "/docs",
    redoc_url   = "/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins  = ["*"],
    allow_methods  = ["*"],
    allow_headers  = ["*"],
)

from api.auth import initialize_auth
from api.routes.authority import router as authority_router
from api.routes.verify import router
app.include_router(router)
app.include_router(authority_router)


app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

@app.on_event("startup")
async def startup():
    """Initialize registry and verify Qdrant on startup."""
    from engine.registry import initialize_registry
    initialize_registry()
    logger.info("Verixia API started.")
    logger.info("Docs available at http://localhost:8790/docs")


@app.get("/", include_in_schema=False)
async def root():
    return {
        "product": "Verixia",
        "tagline": "AI outputs, proven.",
        "version": "0.1.0",
        "docs":    "/docs",
        "verify":  "POST /v1/verify",
        "stats":   "GET /v1/stats",
    }


from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

app.mount("/static", StaticFiles(directory="api/static"), name="static")


@app.get("/ui", include_in_schema=False)
async def ui():
    return FileResponse("api/static/index.html")
