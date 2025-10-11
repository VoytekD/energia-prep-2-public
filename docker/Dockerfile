# syntax=docker/dockerfile:1.7

# ──────────────────────────────────────────────────────────────────────────────
# Baza
# ──────────────────────────────────────────────────────────────────────────────
FROM python:3.12-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Europe/Warsaw
RUN apt-get update && apt-get install -y --no-install-recommends \
      locales tzdata ca-certificates curl tini \
 && echo "pl_PL.UTF-8 UTF-8" > /etc/locale.gen \
 && locale-gen \
 && update-ca-certificates \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /app

# ──────────────────────────────────────────────────────────────────────────────
# Warstwa dependencies (cache’owana)
# instalujemy dokładnie to, co w [project.dependencies] z pyproject.toml
# ──────────────────────────────────────────────────────────────────────────────
FROM base AS deps
COPY pyproject.toml /app/pyproject.toml
RUN python - <<'PY'
import tomllib, subprocess, sys, pathlib
py = pathlib.Path("/app/pyproject.toml").read_bytes()
deps = tomllib.loads(py.decode())["project"]["dependencies"]
print("[deps] Installing:", *["  - "+d for d in deps], sep="\n")
subprocess.run([sys.executable, "-m", "pip", "install", "--upgrade", "pip"], check=True)
subprocess.run([sys.executable, "-m", "pip", "install", *deps], check=True)
PY

# ──────────────────────────────────────────────────────────────────────────────
# Final
# ──────────────────────────────────────────────────────────────────────────────
FROM base AS final

# znacznik wersji/commita – zmiana = inny digest obrazu (Compose sam zrekreuje kontener)
ARG VCS_REF=dev
ENV VCS_REF=${VCS_REF}
LABEL org.opencontainers.image.revision="${VCS_REF}"

# Zainstalowane zależności z warstwy deps
COPY --from=deps /usr/local /usr/local

# Kod i zasoby projektu:
COPY src/ /app/src/
COPY sql/ /app/sql/
COPY data/ /app/data/
COPY config_form.yaml /app/config_form.yaml
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Uruchamianie pakietu przez PYTHONPATH (nic nie „pipujemy” z lokalnego src,
# więc warstwa deps zostaje cache’owana, a zmiana kodu dotyka tylko copy src/)
ENV PYTHONPATH=/app/src

ENTRYPOINT ["/usr/bin/tini","--","/entrypoint.sh"]
EXPOSE 8003
