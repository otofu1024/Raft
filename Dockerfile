FROM ghcr.io/astral-sh/uv:0.9.2-python3.12-bookworm-slim

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

COPY . .
CMD ["uv", "run", "uvicorn", "app.main:app", "--reload"]
# uvicorn main:app --reload