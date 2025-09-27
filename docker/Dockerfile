# syntax=docker/dockerfile:1.7

FROM python:3.12-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 TZ=Europe/Warsaw
RUN apt-get update && apt-get install -y --no-install-recommends \
      locales tzdata ca-certificates curl tini \
 && echo "pl_PL.UTF-8 UTF-8" > /etc/locale.gen \
 && locale-gen \
 && update-ca-certificates \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /app

# ── Etap: deps (instalujemy tylko dependencies z pyproject.toml) ──
FROM base AS deps
COPY pyproject.toml /app/pyproject.toml

# Wydobądź listę dependencies z [project.dependencies] i zainstaluj je.
# Używamy stdlib 'tomllib' (Py3.11+), oraz cache mount dla pip.
RUN --mount=type=cache,target=/root/.cache/pip \
    python - <<'PY' > /app/deps.txt
import tomllib
with open("pyproject.toml","rb") as f:
    t = tomllib.load(f)
for dep in t.get("project", {}).get("dependencies", []):
    print(dep)
PY
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip setuptools wheel && \
    pip install -r /app/deps.txt

# ── Etap: runtime (kopiujemy kod i resztę artefaktów) ──
FROM base AS runtime
# Przenosimy zainstalowane pakiety Pythona
COPY --from=deps /usr/local /usr/local

# Kod i zasoby projektu:
COPY src/ /app/src/
COPY sql/ /app/sql/
COPY data/ /app/data/
COPY config_form.yaml /app/config_form.yaml
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Uruchamianie pakietu po prostu przez umieszczenie src na PYTHONPATH
ENV PYTHONPATH=/app/src

ENTRYPOINT ["/usr/bin/tini","--","/entrypoint.sh"]
EXPOSE 8003
