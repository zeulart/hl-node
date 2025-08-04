FROM python:3.11-slim

# Copy uv from the official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ARG USERNAME=hluser
ARG USER_UID=10000
ARG USER_GID=$USER_UID

# Install system dependencies
RUN apt-get update -y && apt-get install -y \
    gcc \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Create user
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME

# Set working directory
WORKDIR /app

# Copy project files
COPY pyproject.toml uv.lock ./
COPY src/ ./src/
COPY config/ ./config/

# Install dependencies with uv
RUN uv sync --frozen

# Change ownership to non-root user
RUN chown -R $USERNAME:$USERNAME /app

# Switch to non-root user
USER $USERNAME

# Set Python path
ENV PYTHONPATH=/app/src

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD uv run python -c "import redis.asyncio as redis; import asyncio; asyncio.run(redis.Redis(host='redis').ping())" || exit 1

# Run the streaming service
ENTRYPOINT ["uv", "run", "python", "src/streaming/main.py"]