# Layer 1: Base image and system deps (rarely changes)
FROM python:3.11-slim as base
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
RUN apt-get update && apt-get install -y gcc && apt-get clean && rm -rf /var/lib/apt/lists/*

# Layer 2: User setup (rarely changes)
ARG USERNAME=hluser
ARG USER_UID=10000
ARG USER_GID=$USER_UID
RUN groupadd --gid $USER_GID $USERNAME && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME

# Layer 3: Dependencies (changes when requirements change)
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

# Layer 4: Application code (changes frequently)
COPY main.py consumer.py ./
RUN chown -R $USERNAME:$USERNAME /app

USER $USERNAME
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD uv run python -c "import redis,os; p=open(os.getenv('REDIS_PASSWORD_FILE','/dev/null'),'r').read().strip() if os.getenv('REDIS_PASSWORD_FILE') else os.getenv('REDIS_PASSWORD'); redis.Redis(host='redis',password=p,username='streaming_producer').ping()" || exit 1
ENTRYPOINT ["uv", "run", "python", "main.py"]