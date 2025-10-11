# src/energia_prep2/app_server.py
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from starlette.applications import Starlette
from starlette.responses import RedirectResponse
from starlette.routing import Mount, Route
from starlette.middleware.cors import CORSMiddleware

from energia_prep2.health_api import app as health_app
from energia_prep2.tasks.params_api import app as params_app, ensure_columns_from_config

# ── root logging (spójny format z pipeline)
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)s | energia-prep-2 | %(message)s",
    )
log = logging.getLogger("energia-prep-2")

def _redirect_to_health_slash(request):
    return RedirectResponse(url="/_health/")

@asynccontextmanager
async def lifespan(app: Starlette):
    # Starlette nie odpala startup sub-app z Mount(), więc zrobimy ensure tutaj.
    try:
        ensure_columns_from_config()
        log.info("app_server: ensured columns from config (startup)")
    except Exception as e:
        log.warning("app_server: ensure_columns_from_config() failed: %s", e)
    yield
    # tu ewentualny shutdown

app = Starlette(
    routes=[
        Route("/_health", endpoint=_redirect_to_health_slash),
        Mount("/_health", app=health_app),
        # params API (Volkov/Grafana): /form/{form_name}
        Mount("/", app=params_app),
    ],
    lifespan=lifespan,
)

# CORS pod Cloudflare Tunnel (zawieź do własnych domen, gdy już wiesz)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # PRODUCTION: wpisz konkretne domeny tunelu
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Uvicorn: uruchamiaj z --proxy-headers gdy idzie przez CF
# uvicorn energia_prep2.app_server:app --host 0.0.0.0 --port 8003 --proxy-headers
