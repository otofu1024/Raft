FROM ghcr.io/astral-sh/uv:0.9.26-python3.13-bookworm

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

COPY . .
CMD ["uv", "run", "uvicorn", "src.raft:app", "--host", "0.0.0.0", "--port", "8000"]
# uvicorn main:app --reload