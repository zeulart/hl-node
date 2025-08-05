# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Development Commands

This project uses `uv` as the package manager. Common commands:

- **Install dependencies**: `uv pip install -r requirements.txt` or `uv sync`
- **Run the streaming service**: `python -m src.streaming.main`
- **Run tests**: `pytest` (when tests are added)
- **Install dev dependencies**: `uv pip install -e ".[dev]"`

## High-Level Architecture

This is a **Hyperliquid node log streaming system** designed for ultra-fast, real-time ingestion of trading data from Hyperliquid node logs into Redis streams.

### Core Components:

1. **Main Entry Point** (`src/streaming/main.py`):
   - Monitors Hyperliquid node log directories (e.g., `/home/hluser/hl/data/node_fills/hourly`)
   - Uses inotify for efficient file change detection
   - Implements automatic hourly log rotation detection
   - Tails log files in real-time and pushes new lines to Redis streams

2. **Configuration System** (`src/streaming/config.py`):
   - YAML-based configuration with environment variable overrides
   - Supports multiple producer configurations for different data types
   - Configurable Redis connection pooling and performance tuning
   - Default config location: `/app/config/streaming.yaml`

3. **Data Flow**:
   - Hyperliquid node writes logs to hourly rotated files (format: `YYYYMMDD/HH`)
   - Streaming service tails the latest log file
   - Each log line is pushed to Redis streams (e.g., `node_fills:ALL`)
   - Consumers can process data from Redis in real-time

### Key Design Decisions:

- **Tail-mode implementation**: Focuses on streaming new data only, not historical processing
- **Hourly rotation handling**: Automatically detects and switches to new log files
- **Redis streams**: Chosen for reliability, persistence, and consumer group support
- **Minimal dependencies**: Uses only essential libraries for production stability

## Environment Variables

- `LOG_PATH`: Override the log directory path (default: `/home/hluser/hl/data/node_fills/hourly`)
- `REDIS_HOST`: Redis server host (default: `localhost`)
- `REDIS_PORT`: Redis server port (default: `6379`)
- `LOG_LEVEL`: Logging level (default: `INFO`)
- `COINS`: Comma-separated list of coins to filter (e.g., `BTC,ETH,HYPE`)
- `STREAMING_CONFIG_PATH`: Path to custom configuration file

## Docker Deployment

The service includes a Dockerfile for containerized deployment, designed to run alongside a Hyperliquid node with shared volume access to log directories.