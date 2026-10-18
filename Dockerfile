# PipeLine app image: the CLI (`python -m ingest ...`), defaulting to the API server.
# Dependencies come from uv.lock (--locked), so the image matches what the tests ran against.
FROM python:3.12-slim

# uv as a static binary from its official image, pinned to the version used in development.
COPY --from=ghcr.io/astral-sh/uv:0.11.3 /uv /bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencies first: this layer is reused until pyproject.toml / uv.lock change.
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

COPY src ./src
RUN uv sync --locked --no-dev

# Never run as root inside the container.
RUN useradd --create-home --uid 10001 app
USER app

EXPOSE 8000
ENTRYPOINT ["python", "-m", "ingest"]
CMD ["serve", "--host", "0.0.0.0", "--port", "8000"]
